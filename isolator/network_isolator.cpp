/**
 * This file is © 2015 Mesosphere, Inc. ("Mesosphere"). Mesosphere
 * licenses this file to you solely pursuant to the agreement between
 * Mesosphere and you (if any).  If there is no such agreement between
 * Mesosphere, the following terms apply (and you may not use this
 * file except in compliance with such terms):
 *
 * 1) Subject to your compliance with the following terms, Mesosphere
 * hereby grants you a nonexclusive, limited, personal,
 * non-sublicensable, non-transferable, royalty-free license to use
 * this file solely for your internal business purposes.
 *
 * 2) You may not (and agree not to, and not to authorize or enable
 * others to), directly or indirectly:
 *   (a) copy, distribute, rent, lease, timeshare, operate a service
 *   bureau, or otherwise use for the benefit of a third party, this
 *   file; or
 *
 *   (b) remove any proprietary notices from this file.  Except as
 *   expressly set forth herein, as between you and Mesosphere,
 *   Mesosphere retains all right, title and interest in and to this
 *   file.
 *
 * 3) Unless required by applicable law or otherwise agreed to in
 * writing, Mesosphere provides this file on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied,
 * including, without limitation, any warranties or conditions of
 * TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * 4) In no event and under no legal theory, whether in tort
 * (including negligence), contract, or otherwise, unless required by
 * applicable law (such as deliberate and grossly negligent acts) or
 * agreed to in writing, shall Mesosphere be liable to you for
 * damages, including any direct, indirect, special, incidental, or
 * consequential damages of any character arising as a result of these
 * terms or out of the use or inability to use this file (including
 * but not limited to damages for loss of goodwill, work stoppage,
 * computer failure or malfunction, or any and all other commercial
 * damages or losses), even if Mesosphere has been advised of the
 * possibility of such damages.
 */

#include <mesos/hook.hpp>
#include <mesos/mesos.hpp>
#include <mesos/module.hpp>

#include <mesos/module/hook.hpp>
#include <mesos/module/isolator.hpp>

#include <mesos/slave/isolator.hpp>

#include <process/future.hpp>
#include <process/io.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>
#include <process/subprocess.hpp>

#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/try.hpp>
#include <stout/uuid.hpp>

#include "interface.hpp"
#include "network_isolator.hpp"

using namespace mesos;
using namespace network_isolator;
using namespace process;

using std::string;
using std::vector;

using mesos::slave::ContainerPrepareInfo;
using mesos::slave::Isolator;

const char* ipamClientKey = "ipam_command";
const char* isolatorClientKey = "isolator_command";
const char* pythonPath = "/usr/bin/python";
const char* ipAddressLabelKey = "MesosContainerizer.NetworkSettings.IPAddress";

const char* netgroupsLabelKey = "network_isolator.netgroups";

hashmap<ContainerID, Info*> *infos = NULL;
hashmap<ExecutorID, ContainerID> *executorContainerIds = NULL;
hashmap<ExecutorID, string> *executorNetgroups = NULL;


template <typename InProto, typename OutProto>
static Try<OutProto> runCommand(const string& path, const InProto& command)
{
  vector<string> argv(1);
  argv[0] = path;
  Try<process::Subprocess> child = process::subprocess(
      argv[0],
      argv,
      process::Subprocess::PIPE(),
      process::Subprocess::PIPE(),
      process::Subprocess::PIPE());
  CHECK_SOME(child);

  string jsonCommand = stringify(JSON::Protobuf(command));
  process::io::write(child.get().in().get(), jsonCommand);
  LOG(INFO) << "Sending IP request command to IPAM: " << jsonCommand;

  waitpid(child.get().pid(), NULL, 0);
  string output = process::io::read(child.get().out().get()).get();
  LOG(INFO) << "Got response: " << output << " from " << path;

  Try<JSON::Object> jsonOutput = JSON::parse<JSON::Object>(output);
  if (jsonOutput.isError()) {
    return Error(
        "Error parsing output '" + output + "' to JSON string" +
        jsonOutput.error());
  }

  Try<OutProto> result = protobuf::parse<OutProto>(jsonOutput.get());
  if (jsonOutput.isError()) {
    return Error(
        "Error parsing output '" + output + "' to Protobuf" + result.error());
  }

  return result;
}


Try<Isolator*> CalicoIsolatorProcess::create(const Parameters& parameters)
{
  string ipamClientPath;
  string isolatorClientPath;
  foreach (const Parameter& parameter, parameters.parameter()) {
    if (parameter.key() == ipamClientKey) {
      ipamClientPath = parameter.value();
    } else if (parameter.key() == isolatorClientKey) {
      isolatorClientPath = parameter.value();
    }
  }
  if (ipamClientPath.empty()) {
    LOG(WARNING) << "IPAM path not specified";
    return Error("IPAM path not specified.");
  }
  return new CalicoIsolator(process::Owned<CalicoIsolatorProcess>(
      new CalicoIsolatorProcess(
          ipamClientPath, isolatorClientPath, parameters)));
}


CalicoIsolatorProcess::CalicoIsolatorProcess(
    const std::string& ipamClientPath_,
    const std::string& isolatorClientPath_,
    const Parameters& parameters_)
  : ipamClientPath(ipamClientPath_),
    isolatorClientPath(isolatorClientPath_),
    parameters(parameters_)
{
  Try<string> result = net::getHostname(self().address.ip);
  if (result.isError()) {
    LOG(FATAL) << "Failed to get hostname: " << result.error();
  }
  hostname = result.get();
}


process::Future<Option<ContainerPrepareInfo>> CalicoIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& rootfs,
    const Option<string>& user)
{
  LOG(INFO) << "CalicoIsolator::prepare";

  if (!executorNetgroups->contains(executorInfo.executor_id())) {
    return Failure(
        "netgroup label not found for executor: " +
        executorInfo.executor_id().value());
  }

  vector<string> netgroups =
    strings::tokenize((*executorNetgroups)[executorInfo.executor_id()], ",");

  IPAMRequestIPMessage ipamMessage;
  IPAMRequestIPMessage::Args* ipamArgs = ipamMessage.mutable_args();
  ipamArgs->set_hostname(hostname);
  ipamArgs->set_num_ipv4(1);
  ipamArgs->set_uid(UUID::random().toString());

  foreach (const string& netgroup, netgroups) {
    ipamArgs->add_netgroups(netgroup);
  }

  if (netgroups.size() == 0) {
    LOG(INFO) << "No netgroups assigned";
    //TODO(kapil): Should we assign a "default" netgroup here?
  }

  LOG(INFO) << "Sending IP request command to IPAM";
  Try<IPAMResponse> response =
    runCommand<IPAMRequestIPMessage, IPAMResponse>(ipamClientPath, ipamMessage);
  if (response.isError()) {
    return Failure("Error running IPAM IP request command: " + response.error());
  } else if (response.get().has_error()) {
    return Failure("Error assigning IP " + response.get().error());
  } else if (response.get().ipv4().size() == 0) {
    return Failure("No IPv4 addresses received from IPAM.");
  }

  LOG(INFO) << "Got IP " << response.get().ipv4(0) << " from IPAM.";

  ContainerPrepareInfo prepareInfo;

  Environment::Variable* variable =
    prepareInfo.mutable_environment()->add_variables();
  variable->set_name("LIBPROCESS_IP");
  variable->set_value(response.get().ipv4(0));

  (*infos)[containerId] =
    new Info(response.get().ipv4(0), netgroups, ipamArgs->uid());
  (*executorContainerIds)[executorInfo.executor_id()] = containerId;

  return prepareInfo;
}


process::Future<Nothing> CalicoIsolatorProcess::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  if (infos->contains(containerId)) {
    LOG(FATAL) << "Unknown container id: " << containerId;
  }
  const Info* info = (*infos)[containerId];

  IsolatorIsolateMessage isolatorMessage;
  IsolatorIsolateMessage::Args* isolatorArgs = isolatorMessage.mutable_args();
  isolatorArgs->set_hostname(hostname);
  isolatorArgs->set_container_id(containerId.value());
  isolatorArgs->set_pid(pid);
  isolatorArgs->add_ipv4_addrs(info->ipAddress);
  isolatorArgs->add_ipv6_addrs();
  foreach (const string& netgroup, info->netgroups) {
    isolatorArgs->add_netgroups(netgroup);
  }

  LOG(INFO) << "Sending isolate command to Isolator";
  Try<IsolatorResponse> response =
    runCommand<IsolatorIsolateMessage, IsolatorResponse>(
        isolatorClientPath, isolatorMessage);
  if (response.isError()) {
    return Failure("Error running isolate command: " + response.error());
  } else if (response.get().has_error()) {
    return Failure("Error isolating " + response.get().error());
  }
  return Nothing();
}


process::Future<Nothing> CalicoIsolatorProcess::cleanup(
    const ContainerID& containerId)
{
  if (!infos->contains(containerId)) {
    LOG(WARNING) << "Ignoring cleanup for unknown container " << containerId;
    return Nothing();
  }

  const Info* info = (*infos)[containerId];

  IPAMReleaseIPMessage ipamMessage;
  ipamMessage.mutable_args()->add_ips(info->ipAddress);

  LOG(INFO) << "Requesting IPAM to release IP: " << info->ipAddress;
  Try<IPAMResponse> response =
    runCommand<IPAMReleaseIPMessage, IPAMResponse>(ipamClientPath, ipamMessage);
  if (response.isError()) {
    return Failure("Error releasing IP from IPAM: " + response.error());
  } else if (response.get().has_error()) {
    return Failure("Error releasing IP " + response.get().error());
  }

  IsolatorCleanupMessage isolatorMessage;
  isolatorMessage.mutable_args()->set_hostname(hostname);
  isolatorMessage.mutable_args()->set_container_id(containerId.value());

  Try<IsolatorResponse> isolatorResponse =
    runCommand<IsolatorCleanupMessage, IsolatorResponse>(
        isolatorClientPath, isolatorMessage);
  if (isolatorResponse.isError()) {
    return Failure("Error running cleanup command:" + isolatorResponse.error());
  } else if (isolatorResponse.get().has_error()) {
    return Failure("Error doing cleanup " + isolatorResponse.get().error());
  }

  return Nothing();
}


static Isolator* createCalicoIsolator(const Parameters& parameters)
{
  LOG(INFO) << "Loading Calico Isolator module";

  if (infos == NULL) {
    infos = new hashmap<ContainerID, Info*>();
    CHECK(executorContainerIds == NULL);
    executorContainerIds = new hashmap<ExecutorID, ContainerID>();
    CHECK(executorNetgroups == NULL);
    executorNetgroups = new hashmap<ExecutorID, string>();
  }

  Try<Isolator*> result = CalicoIsolatorProcess::create(parameters);

  if (result.isError()) {
    return NULL;
  }

  return result.get();
}


// TODO(karya): Use the hooks for Task Status labels.
class CalicoHook : public Hook
{
  virtual Result<Labels> slaveRunTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    if (taskInfo.has_labels()) {
      foreach (const Label& label, taskInfo.labels().labels()) {
        if (label.key() == netgroupsLabelKey) {
          (*executorNetgroups)[executorInfo.executor_id()] = label.value();
        }
      }
    }
    return None();
  }

  virtual Result<Labels> slaveTaskStatusLabelDecorator(
      const FrameworkID& frameworkId,
      const TaskStatus& status)
  {
    LOG(INFO) << "CalicoHook::task status label decorator";

    if (!status.has_executor_id()) {
      LOG(WARNING) << "CalicoHook:: task status has no valid executor id";
      return None();
    }

    const ExecutorID executorId = status.executor_id();
    if (!executorContainerIds->contains(executorId)) {
      LOG(WARNING) << "CalicoHook:: no valid container id for: " << executorId;
      return None();
    }

    const ContainerID containerId = executorContainerIds->at(executorId);
    if (infos == NULL || !infos->contains(containerId)) {
      LOG(WARNING) << "CalicoHook:: no valid infos for: " << containerId;
      return None();
    }

    const Info* info = (*infos)[containerId];

    Labels labels;
    if (status.has_labels()) {
      labels.CopyFrom(status.labels());
    }

    // Set IPAddress label.
    Label* label = labels.add_labels();
    label->set_key(ipAddressLabelKey);
    label->set_value(info->ipAddress);

    LOG(INFO) << "CalicoHook:: added label "
              << label->key() << ":" << label->value();
    return labels;
  }
};


static Hook* createCalicoHook(const Parameters& parameters)
{
  return new CalicoHook();
}


// Declares the Calico isolator named
// 'org_apache_mesos_CalicoIsolator'.
mesos::modules::Module<Isolator> com_mesosphere_mesos_CalicoIsolator(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Mesosphere",
    "support@mesosphere.com",
    "Calico Isolator module.",
    NULL,
    createCalicoIsolator);


// Declares the Calico hook module 'org_apache_mesos_CalicoHook'.
mesos::modules::Module<Hook> com_mesosphere_mesos_CalicoHook(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Mesosphere",
    "support@mesosphere.com",
    "Calico Hook module.",
    NULL,
    createCalicoHook);
