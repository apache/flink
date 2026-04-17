/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export interface RescalesOverview {
  rescalesCounts: RescalesCounts;
  latest: LatestRescales;
}

export interface RescalesCounts {
  ignored: number;
  inProgress: number;
  completed: number;
  failed: number;
}

export interface LatestRescales {
  completed: BriefJobRescaleDetails | null;
  failed: BriefJobRescaleDetails | null;
  ignored: BriefJobRescaleDetails | null;
}

export type RescalesHistory = BriefJobRescaleDetails[];

export interface BriefJobRescaleDetails {
  rescaleUuid: string;
  resourceRequirementsUuid: string;
  rescaleAttemptId: number;
  vertices: { [jobVertexId: string]: VertexParallelismRescaleInfo };
  slots: { [slotSharingGroupId: string]: SlotSharingGroupRescaleInfo };
  schedulerStates: SchedulerState[];
  startTimestampInMillis: number;
  endTimestampInMillis: number;
  terminalState: string;
  triggerCause: string;
  terminatedReason: string;
}

export interface JobRescaleDetails extends BriefJobRescaleDetails {}

export interface VertexParallelismRescaleInfo {
  jobVertexId: string;
  jobVertexName: string;
  slotSharingGroupId: string;
  slotSharingGroupName: string;
  desiredParallelism: number;
  sufficientParallelism: number;
  preRescaleParallelism: number;
  postRescaleParallelism: number;
}

export interface SlotSharingGroupRescaleInfo {
  slotSharingGroupId: string;
  slotSharingGroupName: string;
  requestResourceProfile: ResourceProfileInfo;
  desiredSlots: number;
  minimalRequiredSlots: number;
  preRescaleSlots: number;
  postRescaleSlots: number;
  acquiredResourceProfile: ResourceProfileInfo;
}

export interface ResourceProfileInfo {
  cpuCores: number;
  taskHeapMemory: number;
  taskOffHeapMemory: number;
  managedMemory: number;
  networkMemory: number;
  extendedResources: { [key: string]: unknown };
}

export interface SchedulerState {
  state: string;
  enterTimestampInMillis: number;
  leaveTimestampInMillis: number;
  durationInMillis: number;
  stringifiedException: string;
}

export interface JobRescaleConfigInfo {
  rescaleHistoryMax: number;
  schedulerExecutionMode: string;
  submissionResourceWaitTimeoutInMillis: number;
  submissionResourceStabilizationTimeoutInMillis: number;
  slotIdleTimeoutInMillis: number;
  executingCooldownTimeoutInMillis: number;
  executingResourceStabilizationTimeoutInMillis: number;
  maximumDelayForTriggeringRescaleInMillis: number;
  rescaleOnFailedCheckpointCount: number;
}
