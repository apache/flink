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

import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, Input } from '@angular/core';

import {
  BackpressureOperatorInfo,
  BusyOperatorInfo,
  CpuConsumerInfo,
  GcTaskInfo,
  SourceLagInfo
} from '@flink-runtime-web/interfaces';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTableModule } from 'ng-zorro-antd/table';

type TopNSection = 'backpressure' | 'busy' | 'sourceLag' | 'cpu' | 'gc';

@Component({
  selector: 'flink-topn-metrics',
  templateUrl: './topn-metrics.component.html',
  styleUrls: ['./topn-metrics.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: true,
  imports: [CommonModule, NzCardModule, NzGridModule, NzTableModule, NzProgressModule]
})
export class TopNMetricsComponent {
  @Input() topBackpressureOperators: BackpressureOperatorInfo[] = [];
  @Input() topBusyOperators: BusyOperatorInfo[] = [];
  @Input() topLaggingSources: SourceLagInfo[] = [];
  @Input() topCpuConsumers: CpuConsumerInfo[] = [];
  @Input() topGcIntensiveTasks: GcTaskInfo[] = [];

  /**
   * Per-section collapse state. Primary-triage sections (backpressure / busy / source lag) are
   * expanded by default because they are the first-line diagnostic signals; JVM-level sections
   * (CPU load / GC time) are collapsed by default and shown as auxiliary diagnostics.
   */
  collapsed: Record<TopNSection, boolean> = {
    backpressure: false,
    busy: false,
    sourceLag: false,
    cpu: true,
    gc: true
  };

  toggle(section: TopNSection): void {
    this.collapsed[section] = !this.collapsed[section];
  }

  formatPercentage(value: number): string {
    return `${value.toFixed(2)}%`;
  }

  /** Render a lag metric that may be unavailable (`null`). */
  formatLag(value: number | null, unit: 'records' | 'ms'): string {
    if (value == null) {
      return 'n/a';
    }
    if (unit === 'records') {
      return Math.round(value).toLocaleString();
    }
    return `${Math.round(value).toLocaleString()} ms`;
  }
}
