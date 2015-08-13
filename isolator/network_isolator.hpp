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

#ifndef __NETWORK_ISOLATOR_HPP__
#define __NETWORK_ISOLATOR_HPP__

#include <mesos/mesos.hpp>

#include <mesos/slave/isolator.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/try.hpp>
#include <stout/option.hpp>

namespace mesos {

struct Info
{
  Info(const std::string& _ipAddress,
       const std::vector<std::string>& _netgroups,
       const std::string& _uid)
    : ipAddress(_ipAddress),
      netgroups(_netgroups),
      uid(_uid) {}

  // The IP address to assign to the container, or NONE for auto-assignment.
  const std::string ipAddress;

  // The network profile name to assign to the container, or NONE for the
  // default.
  const std::vector<std::string> netgroups;

  // Unique identifier assigned to each IPAM IP request.
  const std::string uid;
};


class CalicoIsolatorProcess : public process::Process<CalicoIsolatorProcess>
{
public:
  static Try<mesos::slave::Isolator*> create(
      const Parameters& parameters);

  ~CalicoIsolatorProcess() {}

  process::Future<Option<mesos::slave::ContainerPrepareInfo>> prepare(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& rootfs,
      const Option<std::string>& user);

  process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid);

  process::Future<Nothing> cleanup(
      const ContainerID& containerId);

private:
  CalicoIsolatorProcess(
      const std::string& ipamClientPath_,
      const std::string& isolatorClientPath_,
      const Parameters& parameters_);

  const std::string ipamClientPath;
  const std::string isolatorClientPath;
  const Parameters parameters;
  std::string hostname;
};


class CalicoIsolator : public mesos::slave::Isolator
{
public:
  CalicoIsolator(process::Owned<CalicoIsolatorProcess> process_)
    : process(process_)
  {
    spawn(CHECK_NOTNULL(process.get()));
  }

  virtual ~CalicoIsolator()
  {
    terminate(process.get());
    wait(process.get());
  }

  virtual process::Future<Option<int>> namespaces()
  {
    return CLONE_NEWNET;
  }

  virtual process::Future<Nothing> recover(
      const std::list<mesos::slave::ContainerState>& states,
      const hashset<ContainerID>& orphans)
  {
    return Nothing();
  }

  virtual process::Future<Option<mesos::slave::ContainerPrepareInfo>> prepare(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& rootfs,
      const Option<std::string>& user)
  {
    return dispatch(process.get(),
                    &CalicoIsolatorProcess::prepare,
                    containerId,
                    executorInfo,
                    directory,
                    rootfs,
                    user);
  }

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid)
  {
    return dispatch(process.get(),
                    &CalicoIsolatorProcess::isolate,
                    containerId,
                    pid);
  }

  virtual process::Future<mesos::slave::ContainerLimitation> watch(
      const ContainerID& containerId)
  {
    return process::Future<mesos::slave::ContainerLimitation>();
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
    return dispatch(process.get(),
                    &CalicoIsolatorProcess::cleanup,
                    containerId);
  }

private:
  process::Owned<CalicoIsolatorProcess> process;
  const Parameters parameters;
};

} // namespace mesos {

#endif // #ifdef __NETWORK_ISOLATOR_HPP__
