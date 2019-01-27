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

export interface ResourceProfile {
  cpu_cores: number;
  heap_memory: number;
  direct_memory: number;
  native_memory: number;
  network_memory: number;
  managed_memory: number;
}

export interface Task {
  vertex_id: string;
  task_name: string;
  subtask: number;
  attempt: number;
}

export interface PendingSlotRequest {
  id: string;
  resource_profile: ResourceProfile;
  start_time: any;
  sharing_id: string;
  'co-location_id'?: string;
  tasks: Task[];
}

export interface JobPendingSlotsInterface {
  'pending-slot-requests': PendingSlotRequest[];
}
