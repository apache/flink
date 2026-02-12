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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

import { CpuConsumer, BackpressureOperator, GcIntensiveTask } from '@flink-runtime-web/interfaces/top-n-metrics';

@Component({
  selector: 'flink-top-n-metrics',
  standalone: true,
  template: `
    <div class="top-n-metrics" *ngIf="topCpuConsumers?.length || topBackpressureOperators?.length || topGcIntensiveTasks?.length">
      <h3>Top N Metrics</h3>
      
      <div class="metric-section" *ngIf="topCpuConsumers?.length">
        <h4>Top {{ topCpuConsumers.length }} CPU Consumers</h4>
        <ul>
          <li *ngFor="let cpu of topCpuConsumers">
            {{ cpu.taskName }} (Subtask {{ cpu.subtaskId }}): {{ cpu.cpuPercentage | number:'1.2' }}% CPU
          </li>
        </ul>
      </div>

      <div class="metric-section" *ngIf="topBackpressureOperators?.length">
        <h4>Top {{ topBackpressureOperators.length }} Backpressure Operators</h4>
        <ul>
          <li *ngFor="let bp of topBackpressureOperators">
            {{ bp.operatorName }} (Subtask {{ bp.subtaskId }}): {{ bp.backpressureRatio | percent }}
          </li>
        </ul>
      </div>

      <div class="metric-section" *ngIf="topGcIntensiveTasks?.length">
        <h4>Top {{ topGcIntensiveTasks.length }} GC Intensive Tasks</h4>
        <ul>
          <li *ngFor="let gc of topGcIntensiveTasks">
            {{ gc.taskName }}: {{ gc.gcTimePercentage | number:'1.2' }}% GC Time
          </li>
        </ul>
      </div>
    </div>
  `,
  styles: [`
    .top-n-metrics {
      padding: 16px;
      margin: 16px 0;
      background-color: #f5f5f5;
      border-radius: 4px;
    }
    .metric-section {
      margin: 12px 0;
    }
    h3 {
      margin: 0 0 12px 0;
    }
    h4 {
      margin: 8px 0;
    }
    ul {
      margin: 4px 0;
      padding-left: 20px;
    }
  `],
  imports: [CommonModule]
})
export class TopNMetricsComponent {
  @Input() topCpuConsumers: CpuConsumer[] = [];
  @Input() topBackpressureOperators: BackpressureOperator[] = [];
  @Input() topGcIntensiveTasks: GcIntensiveTask[] = [];
}
