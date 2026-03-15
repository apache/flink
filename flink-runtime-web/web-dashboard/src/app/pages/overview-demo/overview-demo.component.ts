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

import { Component, OnInit } from '@angular/core';

import { CpuConsumerInfo, BackpressureOperatorInfo, GcTaskInfo } from '@flink-runtime-web/interfaces';

@Component({
  selector: 'flink-overview-demo',
  templateUrl: './overview-demo.component.html',
  styleUrls: ['./overview-demo.component.less']
})
export class OverviewDemoComponent implements OnInit {
  demoCpuConsumers: CpuConsumerInfo[] = [];
  demoBackpressureOperators: BackpressureOperatorInfo[] = [];
  demoGcIntensiveTasks: GcTaskInfo[] = [];

  ngOnInit(): void {
    // Initialize demo data
    this.demoCpuConsumers = [
      {
        subtaskId: 0,
        taskName: 'Source: Kafka',
        operatorName: 'Kafka Source',
        cpuPercentage: 95.5,
        taskManagerId: 'container_123'
      },
      {
        subtaskId: 1,
        taskName: 'Map: Process',
        operatorName: 'ProcessFunction',
        cpuPercentage: 88.2,
        taskManagerId: 'container_124'
      },
      {
        subtaskId: 2,
        taskName: 'Sink: HDFS',
        operatorName: 'HDFS Sink',
        cpuPercentage: 82.7,
        taskManagerId: 'container_125'
      },
      {
        subtaskId: 3,
        taskName: 'Window',
        operatorName: 'WindowFunction',
        cpuPercentage: 76.3,
        taskManagerId: 'container_123'
      },
      {
        subtaskId: 4,
        taskName: 'Aggregate',
        operatorName: 'AggregateFunction',
        cpuPercentage: 71.9,
        taskManagerId: 'container_126'
      }
    ];

    this.demoBackpressureOperators = [
      {
        operatorId: 'op_456',
        operatorName: 'Map',
        backpressureRatio: 0.85,
        subtaskId: 1
      },
      {
        operatorId: 'op_789',
        operatorName: 'Window',
        backpressureRatio: 0.72,
        subtaskId: 3
      },
      {
        operatorId: 'op_234',
        operatorName: 'Filter',
        backpressureRatio: 0.58,
        subtaskId: 2
      },
      {
        operatorId: 'op_567',
        operatorName: 'Join',
        backpressureRatio: 0.45,
        subtaskId: 4
      },
      {
        operatorId: 'op_890',
        operatorName: 'Aggregate',
        backpressureRatio: 0.32,
        subtaskId: 5
      }
    ];

    this.demoGcIntensiveTasks = [
      {
        taskId: 'task_789',
        taskName: 'ProcessFunction',
        gcTimePercentage: 45.2,
        taskManagerId: 'container_123'
      },
      {
        taskId: 'task_456',
        taskName: 'WindowFunction',
        gcTimePercentage: 38.7,
        taskManagerId: 'container_124'
      },
      {
        taskId: 'task_234',
        taskName: 'JoinFunction',
        gcTimePercentage: 32.1,
        taskManagerId: 'container_125'
      },
      {
        taskId: 'task_567',
        taskName: 'AggregateFunction',
        gcTimePercentage: 28.5,
        taskManagerId: 'container_126'
      },
      {
        taskId: 'task_890',
        taskName: 'FilterFunction',
        gcTimePercentage: 24.3,
        taskManagerId: 'container_127'
      }
    ];
  }
}
