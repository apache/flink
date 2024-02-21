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

export interface TaskManagerList {
  taskmanagers: TaskManagersItem[];
}

export interface TaskManagerDetail {
  id: string;
  path: string;
  dataPort: number;
  timeSinceLastHeartbeat: number;
  slotsNumber: number;
  freeSlots: number;
  hardware: Hardware;
  metrics: Metrics;
  memoryConfiguration: MemoryConfiguration;
  blocked?: boolean;
  freeResource: Resources;
  totalResource: Resources;
  allocatedSlots: AllocatedSlot[];
}

export interface AllocatedSlot {
  jobId: string;
  resource: Resources;
}

export interface Resources {
  cpuCores: number;
  taskHeapMemory: number;
  taskOffHeapMemory: number;
  managedMemory: number;
  networkMemory: number;
}

export interface TaskManagerLogItem {
  name: string;
  size: number;
  mtime: number;
}

export interface TaskManagerLogDetail {
  data: string;
  url: string;
}

export interface TaskManagersItem {
  id: string;
  path: string;
  dataPort: number;
  timeSinceLastHeartbeat: number;
  slotsNumber: number;
  freeSlots: number;
  hardware: Hardware;
  blocked?: boolean;
}

interface Hardware {
  cpuCores: number;
  physicalMemory: number;
  freeMemory: number;
  managedMemory: number;
}

interface Metrics {
  heapUsed: number;
  heapCommitted: number;
  heapMax: number;
  nonHeapUsed: number;
  nonHeapCommitted: number;
  nonHeapMax: number;
  directCount: number;
  directUsed: number;
  directMax: number;
  mappedCount: number;
  mappedUsed: number;
  mappedMax: number;
  memorySegmentsAvailable: number;
  memorySegmentsTotal: number;
  garbageCollectors: GarbageCollectorsItem[];
}

interface MemoryConfiguration {
  frameworkHeap: number;
  frameworkOffHeap: number;
  jvmMetaspace: number;
  jvmOverhead: number;
  managedMemory: number;
  networkMemory: number;
  taskHeap: number;
  taskOffHeap: number;
  totalFlinkMemory: number;
  totalProcessMemory: number;
}

interface GarbageCollectorsItem {
  name: string;
  count: number;
  time: number;
}

export interface TaskManagerThreadDump {
  threadInfos: TaskManagerThreadInfo[];
}

interface TaskManagerThreadInfo {
  threadName: string;
  stringifiedThreadInfo: string;
}
