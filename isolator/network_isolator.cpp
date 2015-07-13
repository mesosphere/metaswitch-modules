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
#include <process/owned.hpp>
#include <process/subprocess.hpp>

#include <stout/try.hpp>
#include <stout/stringify.hpp>
#include <stout/hashmap.hpp>
#include <stout/option.hpp>

using namespace mesos;

using mesos::slave::Isolator;

struct Info
{
  Info(const Option<std::string>& _ipAddress,
      const Option<std::string>& _profile)
    : ipAddress(_ipAddress),
      profile(_profile) {}

  // The IP address to assign to the container, or NONE for auto-assignment.
  const Option<std::string> ipAddress;

  // The network profile name to assign to the container, or NONE for the
  // default.
  const Option<std::string> profile;
};

const char* initializationKey = "initialization_command";
const char* cleanupKey = "cleanup_command";
const char* isolateKey = "isolate_command";
const char* pythonPath = "/usr/bin/python";

hashmap<ContainerID, Info*> *infos = NULL;
hashmap<ExecutorID, ContainerID> *executors = NULL;

class CalicoIsolatorProcess : public mesos::slave::IsolatorProcess
{
public:
  static Try<mesos::slave::Isolator*> create(const Parameters& parameters)
  {
    return new Isolator(process::Owned<IsolatorProcess>(
        new CalicoIsolatorProcess(parameters)));
  }

  virtual ~CalicoIsolatorProcess() {}

  virtual process::Future<Option<int>> namespaces()
  {
    return CLONE_NEWNET;
  }

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ExecutorRunState>& states,
      const hashset<ContainerID>& orphans)
  {
    return Nothing();
  }

  virtual process::Future<Option<CommandInfo>> prepare(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& rootfs,
      const Option<std::string>& user)
  {
    LOG(INFO) << "CalicoIsolator::prepare";

#if 0
    std::string ipAddress = "auto";
    std::string profile = "none";
    foreach (const Environment_Variable& var,
             executorInfo.command().environment().variables()) {
      LOG(INFO) << "ENV: " << var.name() << "=" << var.value();
      if (var.name() == "CALICO_IP") {
        ipAddress = var.value();
      }
      else if (var.name() == "CALICO_PROFILE") {
        profile = var.value();
      }
    }
    infos[containerId] = new Info(ipAddress, profile);
#endif

    (*executors)[executorInfo.executor_id()] = containerId;

    foreach (const Parameter& parameter, parameters.parameter()) {
      if (parameter.key() == initializationKey) {
        CommandInfo commandInfo;
        commandInfo.set_value(parameter.value());
        return commandInfo;
      }
    }
    return None();
  }

  // TODO(kapil): File a Mesos ticket to extend isolate() signature
  // to include ExecutorInfo.
  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid)
  {
    const Info* info = (*infos)[containerId];
    foreach (const Parameter& parameter, parameters.parameter()) {
      if (parameter.key() == isolateKey) {
        std::vector<std::string> argv(7);
        argv[0] = "python";
        argv[1] = parameter.value();
        argv[2] = "isolate";
        argv[3] = stringify(pid);
        argv[4] = containerId.value();
        argv[5] = stringify(info->ipAddress.get());
        argv[6] = stringify(info->profile.get());
        Try<process::Subprocess> child = process::subprocess(pythonPath, argv);
        CHECK_SOME(child);
        waitpid(child.get().pid(), NULL, 0);
        break;
      }
    }
    return Nothing();
  }

  virtual process::Future<mesos::slave::Limitation> watch(
      const ContainerID& containerId)
  {
    return process::Future<mesos::slave::Limitation>();
  }

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources)
  {
    return Nothing();
  }

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId)
  {
    return ResourceStatistics();
  }

  virtual process::Future<Nothing> cleanup(
      const ContainerID& containerId)
  {
    if (!infos->contains(containerId)) {
      LOG(WARNING) << "Ignoring cleanup for unknown container " << containerId;
      return Nothing();
    }
    infos->erase(containerId);
    foreach (const Parameter& parameter, parameters.parameter()) {
      if (parameter.key() == cleanupKey) {
        std::vector<std::string> argv(4);
        argv[0] = "python";
        argv[1] = parameter.value();
        argv[2] = "cleanup";
        argv[3] = containerId.value();
        Try<process::Subprocess> child = process::subprocess(pythonPath, argv);
        CHECK_SOME(child);
        waitpid(child.get().pid(), NULL, 0);
        break;
      }
    }
    return Nothing();
  }

private:
  CalicoIsolatorProcess(const Parameters& parameters_)
    : parameters(parameters_) {}

  const Parameters parameters;
};


static Isolator* createCalicoIsolator(const Parameters& parameters)
{
  LOG(INFO) << "Loading Calico Isolator module";
  if (infos == NULL) {
    infos = new hashmap<ContainerID, Info*>();
    CHECK(executors == NULL);
    executors = new hashmap<ExecutorID, ContainerID>();
  }

  Try<Isolator*> result = CalicoIsolatorProcess::create(parameters);
  if (result.isError()) {
    return NULL;
  }
  return result.get();
}



class CalicoHook : public Hook
{
public:
  // We need this hook to set "LIBPROCESS_IP" environment for the
  // executor. This would force the executor to bind to the given IP
  // instead of binding to the loopback IP.
  // In this hook, we create a new environment variable "LIBPROCESS_IP"
  // TODO(kapil): File a Mesos ticket to extend this signature
  // to also include ContainerID.
  virtual Result<Environment> slaveExecutorEnvironmentDecorator(
      const ExecutorInfo& executorInfo)
  {
    LOG(INFO) << "CalicoHook::slaveExecutorEnvironmentDecorator";

    Environment environment;

    if (executorInfo.command().has_environment()) {
      environment.CopyFrom(executorInfo.command().environment());
    }

    std::string ipAddress = "auto";
    std::string profile = "none";
    foreach (const Environment_Variable& var,
             executorInfo.command().environment().variables()) {
      LOG(INFO) << "ENV: " << var.name() << "=" << var.value();
      if (var.name() == "CALICO_IP") {
        ipAddress = var.value();
      }
      else if (var.name() == "CALICO_PROFILE") {
        profile = var.value();
      }
    }

    std::string libprocessIP = "0.0.0.0";
    if (ipAddress == "auto") {
     // TODO(kapil): Contact Calico IPAM to calculate libprocessIP.
    }

    foreach (const Environment_Variable& var,
             executorInfo.command().environment().variables()) {
      if (var.name() != "LIBPROCESS_IP") {
        environment.add_variables()->CopyFrom(var);
      }
    }

    Environment::Variable* variable = environment.add_variables();
    variable->set_name("LIBPROCESS_IP");
    variable->set_value(libprocessIP);

    // TODO(kapil): Update CALICO_IP with the correct IP (if needed).

    if (!executors->contains(executorInfo.executor_id())) {
      LOG(WARNING) << "Unknown executor " << executorInfo.executor_id();
      return Error("Unknown executor");
    }
    const ContainerID containerId = executors->at(executorInfo.executor_id());
    (*infos)[containerId] = new Info(ipAddress, profile);

    return environment;
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
