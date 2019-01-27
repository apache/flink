/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

export interface JobStatusCountsInterface {
  'CREATED': number;
  'SCHEDULED': number;
  'CANCELED': number;
  'DEPLOYING': number;
  'RUNNING': number;
  'CANCELING': number;
  'FINISHED': number;
  'FAILED': number;
  'RECONCILING': number;
}

interface TimestampsStatus {
  FINISHED: number;
  FAILING: number;
  SUSPENDING: number;
  RECONCILING: number;
  CREATED: number;
  RESTARTING: number;
  CANCELLING: number;
  FAILED: number;
  CANCELED: number;
  RUNNING: number;
  SUSPENDED: number;
}

export interface JobDetailInterface {
  'jid': string;
  'name': string;
  'isStoppable': boolean;
  'state': string;
  'start-time': number;
  'end-time': number;
  'duration': number;
  'now': number;
  'timestamps': TimestampsStatus;
  'vertices': VerticesItemInterface[];
  'status-counts': JobStatusCountsInterface;
  'plan': Plan;
}

interface Plan {
  jid: string;
  name: string;
  nodes: NodesItemInterface[];
}

interface InputsItem {
  num: number;
  id: string;
  ship_strategy: string;
  exchange: string;
}

export interface VerticesItemInterface {
  id: string;
  name: string;
  parallelism: number;
  status: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  tasks: TasksStatus;
  metrics: JobMetricsStatus;
}

interface TasksStatus {
  FINISHED: number;
  SCHEDULED: number;
  CANCELED: number;
  CREATED: number;
  DEPLOYING: number;
  RUNNING: number;
  FAILED: number;
  RECONCILING: number;
  CANCELING: number;
}

export interface JobMetricsStatus {
  'buffers-in-pool-usage-max': number;
  'buffers-in-pool-usage-max-complete': boolean;
  'buffers-out-pool-usage-max': number;
  'buffers-out-pool-usage-max-complete': boolean;
  'delay': number;
  'delay-complete': boolean;
  'read-bytes': number;
  'read-bytes-complete': boolean;
  'read-records': number;
  'read-records-complete': boolean;
  'tps': number;
  'tps-complete': boolean;
  'write-bytes': number;
  'write-bytes-complete': boolean;
  'write-records': number;
  'write-records-complete': boolean;
}

export interface NodesItemInterface {
  'id': string;
  'parallelism': number;
  'operator': string;
  'operator_strategy': string;
  'description': string;
  'inputs'?: InputsItem[];
  'optimizer_properties': {};
  width?: number;
  height?: number;
}

export interface NodesItemCorrectInterface extends NodesItemInterface {
  detail: VerticesItemInterface;
}

export interface NodesItemLinkInterface {
  id: string;
  source: string;
  target: string;
}

export interface JobDetailCorrectInterface extends JobDetailInterface {
  verticesDetail: VerticesDetailInterface;
  'plan': {
    jid: string;
    name: string;
    nodes: NodesItemCorrectInterface[];
    links: NodesItemLinkInterface[];
  };
}

export interface VerticesDetailInterface {
  vertices: VerticesItem[];
  operators: OperatorsItem[];
}

export interface VerticesItem {
  id: string;
  name: string;
  parallelism?: number;
  subtask_metrics: SubtaskMetricsItem[];
  metrics?: VerticesMetrics;
}

export interface VerticesMetrics {
  'read-bytes': number;
  'read-bytes-complete': boolean;
  'write-bytes': number;
  'write-bytes-complete': boolean;
  'read-records': number;
  'read-records-complete': boolean;
  'write-records': number;
  'write-records-complete': boolean;
  'buffers-in-pool-usage-max': number;
  'buffers-in-pool-usage-max-complete': boolean;
  'buffers-out-pool-usage-max': number;
  'buffers-out-pool-usage-max-complete': boolean;
  tps: number;
  'tps-complete': boolean;
  delay: number;
  'delay-complete': boolean;
} {

}

export interface SubtaskMetricsItem {
  'buffers.inputQueueLength': string;
  'buffers.outputQueueLength': string;
  'buffers.inPoolUsage': string;
  'buffers.outPoolUsage': string;
  numBytesInLocal: string;
  numBytesInRemotePerSecond: string;
  numBytesOutPerSecond: string;
  numBytesInLocalPerSecond: string;
  numBytesOut: string;
  numRecordsIn: string;
  numRecordsOutPerSecond: string;
  numRecordsOut: string;
  numRecordsInPerSecond: string;
  numBuffersOut: string;
  numBytesInRemote: string;
  checkpointAlignmentTime?: string;
  currentInputWatermark?: string;

  [ metric_name: string ]: string;
}

export interface OperatorsItem {
  operator_id: string;
  vertex_id: string;
  name: string;
  inputs: VerticesDetailInputsItem[];
  metric_name: string;
  virtual?: boolean;
}

export interface VerticesDetailInputsItem {
  operator_id: string;
  partitioner: string;
  type_number: number;
}
