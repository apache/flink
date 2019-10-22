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

export interface JobStatusCountsInterface {
  CREATED: number;
  SCHEDULED: number;
  CANCELED: number;
  DEPLOYING: number;
  RUNNING: number;
  CANCELING: number;
  FINISHED: number;
  FAILED: number;
  RECONCILING: number;
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
  jid: string;
  name: string;
  isStoppable: boolean;
  state: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  now: number;
  timestamps: TimestampsStatus;
  vertices: VerticesItemInterface[];
  'status-counts': JobStatusCountsInterface;
  plan: Plan;
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

export interface VerticesLinkInterface extends InputsItem {
  source: string;
  target: string;
  id: string;
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
  metrics: MetricsStatus;
}

export interface VerticesItemRangeInterface extends VerticesItemInterface {
  range: number[];
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

interface MetricsStatus {
  'read-bytes': number;
  'read-bytes-complete': boolean;
  'write-bytes': number;
  'write-bytes-complete': boolean;
  'read-records': number;
  'read-records-complete': boolean;
  'write-records': number;
  'write-records-complete': boolean;
}

export interface NodesItemInterface {
  id: string;
  parallelism: number;
  operator: string;
  operator_strategy: string;
  description: string;
  inputs?: InputsItem[];
  optimizer_properties: {};
  width?: number;
  height?: number;
}

export interface NodesItemCorrectInterface extends NodesItemInterface {
  detail: VerticesItemInterface | undefined;
  lowWatermark?: number;
}

export interface NodesItemLinkInterface {
  id: string;
  source: string;
  target: string;
  width?: number;
  ship_strategy?: string;
  local_strategy?: string;
}

export interface JobDetailCorrectInterface extends JobDetailInterface {
  plan: {
    jid: string;
    name: string;
    nodes: NodesItemCorrectInterface[];
    links: NodesItemLinkInterface[];
  };
}
