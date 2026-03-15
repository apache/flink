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

import { ChangeDetectionStrategy, Component, Input, OnChanges, SimpleChanges } from '@angular/core';

import { Diagnosis, DiagnosticSuggestion } from '@flink-runtime-web/interfaces';

@Component({
  selector: 'flink-diagnosis',
  templateUrl: './diagnosis.component.html',
  styleUrls: ['./diagnosis.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class DiagnosisComponent implements OnChanges {
  @Input() diagnosis: Diagnosis | null = null;
  @Input() loading = false;

  ngOnChanges(changes: SimpleChanges): void {
    console.log('Diagnosis updated:', changes);
  }

  getSeverityClass(severity: string): string {
    switch (severity.toLowerCase()) {
      case 'warning':
        return 'severity-warning';
      case 'error':
        return 'severity-error';
      case 'info':
      default:
        return 'severity-info';
    }
  }

  getSeverityIcon(severity: string): string {
    switch (severity.toLowerCase()) {
      case 'warning':
        return 'exclamation-circle';
      case 'error':
        return 'close-circle';
      case 'info':
      default:
        return 'info-circle';
    }
  }

  formatMetricValue(key: string, value: any): string {
    if (typeof value === 'number') {
      if (key.toLowerCase().includes('usage') || key.toLowerCase().includes('ratio')) {
        return `${(value * 100).toFixed(2)}%`;
      } else if (key.toLowerCase().includes('count')) {
        return value.toLocaleString();
      } else {
        return value.toFixed(2);
      }
    }
    return String(value);
  }

  getMetricLabel(key: string): string {
    const labels: { [key: string]: string } = {
      cpuUsage: 'CPU Usage',
      heapUsageRatio: 'Heap Usage',
      gcCount: 'GC Count',
      maxBackpressureRatio: 'Max Backpressure'
    };
    return labels[key] || key;
  }
}
