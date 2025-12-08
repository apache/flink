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

import { JobStatus } from '@flink-runtime-web/interfaces/application-overview';
import { JobsItem } from '@flink-runtime-web/interfaces/job-overview';

interface TimestampsStatus {
  FINISHED: number;
  FAILING: number;
  CREATED: number;
  CANCELLING: number;
  FAILED: number;
  CANCELED: number;
  RUNNING: number;
}

export interface ApplicationDetail {
  id: string;
  name: string;
  status: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  timestamps: TimestampsStatus;
  jobs: JobsItem[];
  'status-counts'?: JobStatus;
}
