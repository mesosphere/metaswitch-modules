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
#include <stout/os/exists.hpp>
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

using mesos::slave::ContainerConfig;
using mesos::slave::ContainerLaunchInfo;
using mesos::slave::Isolator;

static const char* ipamClientKey = "ipam_command";
static const char* isolatorClientKey = "isolator_command";

static hashmap<ContainerID, Info*> *infos = NULL;
static hashmap<ExecutorID, ContainerID> *executorContainerIds = NULL;
static bool isolatorActivated = false;

static Try<Isolator*> networkIsolator = (Isolator*) NULL;

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

  string jsonCommand = stringify(JSON::protobuf(command));

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
  bool ipamPathSpecified = false;
  bool isolatorPathSpecified = false;
  foreach (const Parameter& parameter, parameters.parameter()) {
    if (parameter.key() == ipamClientKey) {
      ipamPathSpecified = true;
      ipamClientPath = parameter.value();
    } else if (parameter.key() == isolatorClientKey) {
      isolatorPathSpecified = true;
      isolatorClientPath = parameter.value();
    }
  }

  if (!ipamPathSpecified) {
    LOG(WARNING) << "IPAM path not specified";
    return Error("IPAM path not specified.");
  }

  if (!isolatorPathSpecified) {
    LOG(WARNING) << "Isolator path not specified";
    return Error("Isolator path not specified.");
  }

  if (os::exists(ipamClientPath) && os::exists(isolatorClientPath)) {
    isolatorActivated = true;
  } else {
    LOG(WARNING) << "IPAM ('" << ipamClientPath << "') or "
                 << "Isolator ('" << isolatorClientPath << "') path doesn't "
                 << "exist; module 'com_mesosphere_mesos_NetworkIsolator' "
                 << "will not be activated";
  }

  return new NetworkIsolator(process::Owned<NetworkIsolatorProcess>(
      new NetworkIsolatorProcess(
          ipamClientPath, isolatorClientPath, parameters)),
      isolatorActivated);
}


NetworkIsolatorProcess::NetworkIsolatorProcess(
    const std::string& ipamClientPath_,
    const std::string& isolatorClientPath_,
    const Parameters& parameters_)
  : ipamClientPath(ipamClientPath_),
    isolatorClientPath(isolatorClientPath_),
    parameters(parameters_)
{}


process::Future<Option<ContainerLaunchInfo>> NetworkIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  LOG(INFO) << "NetworkIsolator::prepare for container: " << containerId;

  const ExecutorInfo executorInfo = containerConfig.executorinfo();
  if (!executorInfo.has_container()) {
    LOG(INFO) << "NetworkIsolator::prepare Ignoring request as "
              << "executorInfo.container is missing for container: "
              << containerId;
    return None();
  }

  if (executorInfo.container().network_infos().size() == 0) {
    LOG(INFO) << "NetworkIsolator::prepare Ignoring request as "
              << "executorInfo.container.network_infos is missing for "
              << "container: " << containerId;
    return None();
  }

  if (executorInfo.container().network_infos().size() > 1) {
    return Failure(
        "NetworkIsolator:: multiple NetworkInfos are not supported.");
  }

  NetworkInfo networkInfo = executorInfo.container().network_infos(0);

  if (networkInfo.has_protocol()) {
    return Failure(
      "NetworkIsolator: NetworkInfo.protocol is deprecated and unsupported.");
  }
  if (networkInfo.has_ip_address()) {
    return Failure(
      "NetworkIsolator: NetworkInfo.ip_address is deprecated and"
      " unsupported.");
  }

  string uid = UUID::random().toString();

  // Two IPAM commands:
  // 1) reserve for IPs the user has specifically asked for.
  // 2) auto-assign IPs.
  // Spin through all IPAddress messages once to get info for each command.
  // Then we'll issue each command if needed.
  IPAMReserveIPMessage reserveMessage;
  IPAMReserveIPMessage::Args* reserveArgs = reserveMessage.mutable_args();

  // Counter of IPs to auto assign.
  int numIPv4 = 0;
  foreach (const NetworkInfo::IPAddress& ipAddress, networkInfo.ip_addresses()) {
    if (ipAddress.has_ip_address() && ipAddress.has_protocol()) {
      return Failure("NetworkIsolator: Cannot include both ip_address and "
                     "protocol in a request.");
    }
    if (ipAddress.has_ip_address()) {
      // Store IP to attempt to reserve.
      reserveArgs->add_ipv4_addrs(ipAddress.ip_address());
    } else if (ipAddress.has_protocol() &&
               ipAddress.protocol() == NetworkInfo::IPv6){
      return Failure("NetworkIsolator: IPv6 is not supported at this time.");
    } else {
      // Either protocol is IPv4, or not included (in which case we default to
      // IPv4 anyway).
      numIPv4++;
    }
  }

  if (!(reserveArgs->ipv4_addrs_size() + numIPv4)) {
    return Failure(
      "NetworkIsolator: Container requires at least one IP address.");
  }

  // All the IP addresses, both reserved and allocated.
  vector<string> allAddresses;

  // Reserve provided IPs first.
  if (reserveArgs->ipv4_addrs_size()) {
    reserveArgs->set_hostname(slaveInfo.hostname());
    reserveArgs->set_uid(uid);
    reserveArgs->mutable_netgroups()->CopyFrom(networkInfo.groups());

    LOG(INFO) << "Sending IP reserve command to IPAM";
    Try<IPAMResponse> response =
      runCommand<IPAMReserveIPMessage, IPAMResponse>(
          ipamClientPath, reserveMessage);
    if (response.isError()) {
      return Failure("Error reserving IPs with IPAM: " + response.error());
    }

    string addresses = "";
    foreach (const string& addr, reserveArgs->ipv4_addrs()) {
      addresses = addresses + addr + " ";
      allAddresses.push_back(addr);
    }
    LOG(INFO) << "IP(s) " << addresses << "reserved with IPAM";
  }

  // Request for IPs the user has asked to auto-assign.
  if (numIPv4) {
    IPAMRequestIPMessage requestMessage;
    IPAMRequestIPMessage::Args* requestArgs = requestMessage.mutable_args();
    requestArgs->set_num_ipv4(numIPv4);
    requestArgs->set_hostname(slaveInfo.hostname());
    requestArgs->set_uid(uid);

    requestArgs->mutable_netgroups()->CopyFrom(networkInfo.groups());

    LOG(INFO) << "Sending IP request command to IPAM";
    Try<IPAMResponse> response =
      runCommand<IPAMRequestIPMessage, IPAMResponse>(
          ipamClientPath, requestMessage);
    if (response.isError()) {
      return Failure("Error allocating IP from IPAM: " + response.error());
    } else if (response.get().ipv4().size() == 0) {
      return Failure("No IPv4 addresses received from IPAM.");
    }

    string addresses = "";
    foreach (const string& addr, response.get().ipv4()) {
      addresses = addresses + addr + " ";
      allAddresses.push_back(addr);
    }
    LOG(INFO) << "IP(s) " << addresses << "allocated with IPAM.";
  }

  vector<string> netgroups;
  foreach (const string& group, networkInfo.groups()) {
    netgroups.push_back(group);
  }

  ContainerLaunchInfo launchInfo;
  launchInfo.set_namespaces(CLONE_NEWNET);

  Environment::Variable* variable =
    launchInfo.mutable_environment()->add_variables();
  variable->set_name("LIBPROCESS_IP");
  // If more than one IP is available, just use the first.  LIBPROCESS just
  // needs one, it doesn't matter which.
  variable->set_value(allAddresses.front());

  (*infos)[containerId] = new Info(allAddresses, netgroups, uid);
  (*executorContainerIds)[executorInfo.executor_id()] = containerId;

  return launchInfo;
}


process::Future<Nothing> NetworkIsolatorProcess::isolate(
    const ContainerID& containerId,
    pid_t pid)
{
  if (!infos->contains(containerId)) {
    LOG(INFO) << "NetworkIsolator::isolate Ignoring isolate request for unknown"
              << " container: " << containerId;
    return Nothing();
  }
  const Info* info = (*infos)[containerId];

  IsolatorIsolateMessage isolatorMessage;
  IsolatorIsolateMessage::Args* isolatorArgs = isolatorMessage.mutable_args();
  isolatorArgs->set_hostname(slaveInfo.hostname());
  isolatorArgs->set_container_id(containerId.value());
  isolatorArgs->set_pid(pid);
  foreach (const string& addr, info->ipAddresses) {
    isolatorArgs->add_ipv4_addrs(addr);
  }
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
    LOG(INFO) << "NetworkIsolator::isolate Ignoring cleanup request for unknown"
              << " container: " << containerId;
    return Nothing();
  }

  const Info* info = (*infos)[containerId];

  string addresses = "";
  IPAMReleaseIPMessage ipamMessage;
  foreach (const string& addr, info->ipAddresses) {
    ipamMessage.mutable_args()->add_ips(addr);
    addresses = addresses + addr + " ";
  }

  LOG(INFO) << "Requesting IPAM to release IPs: " << addresses;
  Try<IPAMResponse> response =
    runCommand<IPAMReleaseIPMessage, IPAMResponse>(ipamClientPath, ipamMessage);
  if (response.isError()) {
    return Failure("Error releasing IP from IPAM: " + response.error());
  }

  IsolatorCleanupMessage isolatorMessage;
  isolatorMessage.mutable_args()->set_hostname(slaveInfo.hostname());
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

  networkIsolator = NetworkIsolatorProcess::create(parameters);

  if (networkIsolator.isError()) {
    return NULL;
  }

  return networkIsolator.get();
}


// TODO(karya): Use the hooks for Task Status labels.
class NetworkHook : public Hook
{
public:
  // We use this hook to get the hostname setup for the Slave.
  virtual Result<Labels> slaveRunTaskLabelDecorator(
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const FrameworkInfo& frameworkInfo,
      const SlaveInfo& slaveInfo)
  {
    if (!isolatorActivated) {
      return None();
    }

    static bool slaveInfoInitialized = false;
    if (!slaveInfoInitialized) {
      NetworkIsolator *isolator = (NetworkIsolator*) networkIsolator.get();
      isolator->updateSlaveInfo(slaveInfo).await();
    }
    return None();
  }

  virtual Result<TaskStatus> slaveTaskStatusDecorator(
      const FrameworkID& frameworkId,
      const TaskStatus& status)
  {
    if (!isolatorActivated) {
      return None();
    }

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
    string addresses = "";
    foreach (const string& addr, info->ipAddresses) {
      NetworkInfo::IPAddress* ipAddress = networkInfo->add_ip_addresses();
      ipAddress->set_ip_address(addr);
      ipAddress->set_protocol(NetworkInfo::IPv4);
      addresses = addresses + addr + " ";
    }

    LOG(INFO) << "NetworkHook:: added ip address(es) " << addresses;
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
