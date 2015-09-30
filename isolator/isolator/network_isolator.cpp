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

hashmap<ContainerID, Info*> *infos = NULL;
hashmap<ExecutorID, ContainerID> *executorContainerIds = NULL;


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

  LOG(INFO) << "Sending command to " + path + ": " << jsonCommand;
  process::io::write(child.get().in().get(), jsonCommand);

  {
    // Temporary hack until Subprocess supports closing stdin.
    // We open /dev/null on fd and dup it over to child's stdin, effectively
    // closing the existing pipe on stdin. We then close the original fd and
    // continue with our business. Child's stdin/out/err are closed in its
    // destructor.
    // TODO(kapil): Replace this block with child.get().closeIn() or
    // equivalient.
    Try<int> fd = os::open("/dev/null", O_WRONLY);
    if (fd.isError()) {
      return Error("Error opening /dev/null:" + fd.error());
    }
    ::dup2(fd.get(), child.get().in().get());
    os::close(fd.get());
  }

  waitpid(child.get().pid(), NULL, 0);
  string output = process::io::read(child.get().out().get()).get();
  LOG(INFO) << "Got response from " << path << ": " << output;

  Try<JSON::Object> jsonOutput_ = JSON::parse<JSON::Object>(output);
  if (jsonOutput_.isError()) {
    return Error(
        "Error parsing output '" + output + "' to JSON string" +
        jsonOutput_.error());
  }
  JSON::Object jsonOutput = jsonOutput_.get();

  Result<JSON::Value> error = jsonOutput.find<JSON::Value>("error");
  if (error.isSome() && !error.get().is<JSON::Null>()) {
    return Error(path + " returned error: " + stringify(error.get()));
  }

  // Protobuf can't parse JSON "null" values; remove error from the object.
  jsonOutput.values.erase("error");

  Try<OutProto> result = protobuf::parse<OutProto>(jsonOutput);
  if (result.isError()) {
    return Error(
        "Error parsing output '" + output + "' to Protobuf" + result.error());
  }

  return result;
}


Try<Isolator*> NetworkIsolatorProcess::create(const Parameters& parameters)
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
  return new NetworkIsolator(process::Owned<NetworkIsolatorProcess>(
      new NetworkIsolatorProcess(
          ipamClientPath, isolatorClientPath, parameters)));
}


NetworkIsolatorProcess::NetworkIsolatorProcess(
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


process::Future<Option<ContainerPrepareInfo>> NetworkIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const std::string& directory,
    const Option<std::string>& user)
{
  LOG(INFO) << "NetworkIsolator::prepare for container: " << containerId;

  if (!executorInfo.has_container()) {
    LOG(INFO) << "NetworkIsolator:: executorInfo.container missing for "
              << "container: " << containerId;
    return None();
  }

  if (executorInfo.container().network_infos().size() > 1) {
    return Failure(
        "NetworkIsolator:: multiple NetworkInfos are not supported.");
  }

  NetworkInfo networkInfo = executorInfo.container().network_infos(0);

  if (networkInfo.groups().size() <= 0) {
    return Failure(
        "netgroup label not found for executor: " +
        executorInfo.executor_id().value());
  }

  if (networkInfo.has_protocol() && networkInfo.has_ip_address()) {
    return Failure(
        "NetworkIsolator: Both protocol and ip_address set in NetworkInfo.");
  }

  string ipAddress;
  string uid = UUID::random().toString();

  vector<string> netgroups;
  foreach (const string& group, networkInfo.groups()) {
    netgroups.push_back(group);
  }

  // Static IP address provided by the framework; use it.
  if (networkInfo.has_ip_address()) {
    ipAddress = networkInfo.ip_address();

    IPAMReserveIPMessage ipamMessage;
    IPAMReserveIPMessage::Args* ipamArgs = ipamMessage.mutable_args();
    ipamArgs->set_hostname(hostname);
    ipamArgs->add_ipv4_addrs(ipAddress);
    ipamArgs->set_uid(uid);
    ipamArgs->mutable_netgroups()->CopyFrom(networkInfo.groups());

    LOG(INFO) << "Sending IP request command to IPAM";
    Try<IPAMResponse> response =
      runCommand<IPAMReserveIPMessage, IPAMResponse>(
          ipamClientPath, ipamMessage);
    if (response.isError()) {
      return Failure("Error reserving IPs with IPAM: " + response.error());
    }

    LOG(INFO) << "IP " << ipAddress << " reserved with IPAM";
  } else {
    IPAMRequestIPMessage ipamMessage;
    IPAMRequestIPMessage::Args* ipamArgs = ipamMessage.mutable_args();
    ipamArgs->set_hostname(hostname);
    ipamArgs->set_num_ipv4(1);
    ipamArgs->set_uid(uid);

    ipamArgs->mutable_netgroups()->CopyFrom(networkInfo.groups());

    LOG(INFO) << "Sending IP request command to IPAM";
    Try<IPAMResponse> response =
      runCommand<IPAMRequestIPMessage, IPAMResponse>(ipamClientPath, ipamMessage);
    if (response.isError()) {
      return Failure("Error allocating IP from IPAM: " + response.error());
    } else if (response.get().ipv4().size() == 0) {
      return Failure("No IPv4 addresses received from IPAM.");
    }

    LOG(INFO) << "Got IP " << response.get().ipv4(0) << " from IPAM.";
    ipAddress = response.get().ipv4(0);
  }

  ContainerPrepareInfo prepareInfo;
  prepareInfo.set_namespaces(CLONE_NEWNET);

  Environment::Variable* variable =
    prepareInfo.mutable_environment()->add_variables();
  variable->set_name("LIBPROCESS_IP");
  variable->set_value(ipAddress);

  (*infos)[containerId] = new Info(ipAddress, netgroups, uid);
  (*executorContainerIds)[executorInfo.executor_id()] = containerId;

  return prepareInfo;
}


process::Future<Nothing> NetworkIsolatorProcess::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  if (!infos->contains(containerId)) {
    LOG(ERROR) << "Unknown container id: " << containerId;
    return Failure("Unknown container id: " + containerId.value());
  }
  const Info* info = (*infos)[containerId];

  IsolatorIsolateMessage isolatorMessage;
  IsolatorIsolateMessage::Args* isolatorArgs = isolatorMessage.mutable_args();
  isolatorArgs->set_hostname(hostname);
  isolatorArgs->set_container_id(containerId.value());
  isolatorArgs->set_pid(pid);
  isolatorArgs->add_ipv4_addrs(info->ipAddress);
  // isolatorArgs->add_ipv6_addrs();
  foreach (const string& netgroup, info->netgroups) {
    isolatorArgs->add_netgroups(netgroup);
  }

  LOG(INFO) << "Sending isolate command to Isolator";
  Try<IsolatorResponse> response =
    runCommand<IsolatorIsolateMessage, IsolatorResponse>(
        isolatorClientPath, isolatorMessage);
  if (response.isError()) {
    return Failure("Error running isolate command: " + response.error());
  }
  return Nothing();
}


process::Future<Nothing> NetworkIsolatorProcess::cleanup(
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
  }

  IsolatorCleanupMessage isolatorMessage;
  isolatorMessage.mutable_args()->set_hostname(hostname);
  isolatorMessage.mutable_args()->set_container_id(containerId.value());

  Try<IsolatorResponse> isolatorResponse =
    runCommand<IsolatorCleanupMessage, IsolatorResponse>(
        isolatorClientPath, isolatorMessage);
  if (isolatorResponse.isError()) {
    return Failure("Error doing cleanup:" + isolatorResponse.error());
  }

  return Nothing();
}


static Isolator* createNetworkIsolator(const Parameters& parameters)
{
  LOG(INFO) << "Loading Network Isolator module";

  if (infos == NULL) {
    infos = new hashmap<ContainerID, Info*>();
    CHECK(executorContainerIds == NULL);
    executorContainerIds = new hashmap<ExecutorID, ContainerID>();
  }

  Try<Isolator*> result = NetworkIsolatorProcess::create(parameters);

  if (result.isError()) {
    return NULL;
  }

  return result.get();
}


// TODO(karya): Use the hooks for Task Status labels.
class NetworkHook : public Hook
{
public:
  virtual Result<TaskStatus> slaveTaskStatusDecorator(
      const FrameworkID& frameworkId,
      const TaskStatus& status)
  {
    LOG(INFO) << "NetworkHook::task status label decorator";

    if (!status.has_executor_id()) {
      LOG(WARNING) << "NetworkHook:: task status has no valid executor id";
      return None();
    }

    const ExecutorID executorId = status.executor_id();
    if (!executorContainerIds->contains(executorId)) {
      LOG(WARNING) << "NetworkHook:: no valid container id for: " << executorId;
      return None();
    }

    const ContainerID containerId = executorContainerIds->at(executorId);
    if (infos == NULL || !infos->contains(containerId)) {
      LOG(WARNING) << "NetworkHook:: no valid infos for: " << containerId;
      return None();
    }

    const Info* info = (*infos)[containerId];

    TaskStatus result;
    NetworkInfo* networkInfo =
      result.mutable_container_status()->add_network_infos();
    networkInfo->set_ip_address(info->ipAddress);

    LOG(INFO) << "NetworkHook:: added ip address " << info->ipAddress;
    return result;
  }
};


static Hook* createNetworkHook(const Parameters& parameters)
{
  return new NetworkHook();
}


// Declares the Network isolator named
// 'org_apache_mesos_NetworkIsolator'.
mesos::modules::Module<Isolator> com_mesosphere_mesos_NetworkIsolator(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Mesosphere",
    "support@mesosphere.com",
    "Network Isolator module.",
    NULL,
    createNetworkIsolator);


// Declares the Network hook module 'org_apache_mesos_NetworkHook'.
mesos::modules::Module<Hook> com_mesosphere_mesos_NetworkHook(
    MESOS_MODULE_API_VERSION,
    MESOS_VERSION,
    "Mesosphere",
    "support@mesosphere.com",
    "Network Hook module.",
    NULL,
    createNetworkHook);
