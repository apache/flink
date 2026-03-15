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

import { Diagnosis } from '@flink-runtime-web/interfaces';

@Component({
  selector: 'flink-diagnosis-demo',
  templateUrl: './diagnosis-demo.component.html',
  styleUrls: ['./diagnosis-demo.component.less']
})
export class DiagnosisDemoComponent implements OnInit {
  selectedScenario = 'gcIssue';
  loading = false;
  currentDiagnosis: Diagnosis | null = null;

  private scenarios: { [key: string]: Diagnosis } = {
    gcIssue: {
      diagnostics: [
        {
          severity: 'warning',
          title: 'High CPU Usage with High Memory Consumption',
          message:
            'High CPU may be caused by frequent GC. Check GC logs or increase heap size.',
          metrics: {
            cpuUsage: 0.85,
            heapUsageRatio: 0.75,
            gcCount: 41092
          },
          actions: [
            'Check GarbageCollectorTime metrics',
            'Review heap size configuration',
            'Analyze GC logs',
            'Consider increasing heap size or optimizing object allocation'
          ]
        }
      ],
      timestamp: new Date().toISOString()
    },
    computation: {
      diagnostics: [
        {
          severity: 'info',
          title: 'High CPU Usage with Normal Memory',
          message: 'High CPU is likely caused by heavy user computation. Check backpressure.',
          metrics: {
            cpuUsage: 0.88,
            heapUsageRatio: 0.45,
            maxBackpressureRatio: 0.35
          },
          actions: [
            'Check backpressure metrics',
            'Review operator implementations for optimization opportunities',
            'Analyze task execution time breakdown'
          ]
        }
      ],
      timestamp: new Date().toISOString()
    },
    ioBottleneck: {
      diagnostics: [
        {
          severity: 'warning',
          title: 'Low CPU with High Backpressure',
          message: 'Possible I/O bottleneck or external dependency delay.',
          metrics: {
            cpuUsage: 0.25,
            maxBackpressureRatio: 0.85
          },
          actions: [
            'Check external system connectivity',
            'Review source/sink performance',
            'Verify network configuration'
          ]
        }
      ],
      timestamp: new Date().toISOString()
    },
    normal: {
      diagnostics: [],
      timestamp: new Date().toISOString()
    }
  };

  ngOnInit(): void {
    this.onScenarioChange();
  }

  onScenarioChange(): void {
    this.loading = true;
    this.currentDiagnosis = null;

    // Simulate loading delay
    setTimeout(() => {
      this.currentDiagnosis = this.scenarios[this.selectedScenario];
      this.loading = false;
    }, 500);
  }
}
