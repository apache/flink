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

export interface JobOverviewInterface {
  jobs: JobsItemInterface[];
}

export interface JobsItemInterface {
  jid: string;
  name: string;
  state: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  'last-modification': number;
  tasks: TaskStatusInterface;
  completed?: boolean;
}

export interface TaskStatusInterface {
  CANCELED: number;
  CANCELING: number;
  CREATED: number;
  DEPLOYING: number;
  FAILED: number;
  FINISHED: number;
  RECONCILING: number;
  RUNNING: number;
  SCHEDULED: number;
  TOTAL: number;
}
