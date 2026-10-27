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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { JobManagerService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobManagerMetricsComponent } from './job-manager-metrics.component';

const gcCount = 'Status.JVM.GarbageCollector.G1_Young_Generation.Count';
const gcTime = 'Status.JVM.GarbageCollector.G1_Young_Generation.Time';

const mockMetrics = {
  'Status.JVM.Memory.Heap.Used': 100,
  'Status.JVM.Memory.Heap.Max': 400,
  'Status.JVM.Memory.Heap.Committed': 200,
  'Status.JVM.Memory.Metaspace.Used': 50,
  'Status.JVM.Memory.Metaspace.Max': 250,
  'Status.JVM.Memory.NonHeap.Committed': 60,
  'Status.JVM.Memory.NonHeap.Used': 40,
  'Status.JVM.Memory.NonHeap.Max': 300,
  'Status.JVM.Memory.Direct.Count': 5,
  'Status.JVM.Memory.Direct.MemoryUsed': 10,
  'Status.JVM.Memory.Direct.TotalCapacity': 10,
  'Status.JVM.Memory.Mapped.Count': 0,
  'Status.JVM.Memory.Mapped.MemoryUsed': 0,
  'Status.JVM.Memory.Mapped.TotalCapacity': 0,
  [gcCount]: 12,
  [gcTime]: 34
};

describe('JobManagerMetricsComponent', () => {
  let fixture: ComponentFixture<JobManagerMetricsComponent>;
  let element: HTMLElement;
  const loadConfig = vi.fn();
  const loadMetricsName = vi.fn();
  const loadMetrics = vi.fn();

  beforeEach(async () => {
    loadConfig.mockReset().mockReturnValue(of([{ key: 'jobmanager.rpc.port', value: '6123' }]));
    loadMetricsName.mockReset().mockReturnValue(of([gcCount, gcTime, 'Status.JVM.CPU.Load']));
    loadMetrics.mockReset().mockReturnValue(of(mockMetrics));
    await TestBed.configureTestingModule({
      imports: [JobManagerMetricsComponent],
      providers: [
        { provide: JobManagerService, useValue: { loadConfig, loadMetricsName, loadMetrics } },
        { provide: StatusService, useValue: { refresh$: of(true) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobManagerMetricsComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads config, garbage-collector names and memory metrics', () => {
    fixture.detectChanges();

    expect(loadConfig).toHaveBeenCalled();
    expect(loadMetricsName).toHaveBeenCalled();
    expect(loadMetrics).toHaveBeenCalled();

    expect(fixture.componentInstance.jmConfig['jobmanager.rpc.port']).toBe('6123');
    // Only the GarbageCollector metric names are retained.
    expect(fixture.componentInstance.listOfGCName).toEqual([gcCount, gcTime]);
    expect(fixture.componentInstance.metrics).toEqual(mockMetrics);

    expect(element.textContent).toContain('Flink Memory Model');
  });

  it('collapses garbage-collector metrics into per-collector rows', () => {
    fixture.detectChanges();

    expect(fixture.componentInstance.listOfGCMetric).toEqual([{ name: 'G1_Young_Generation', count: 12, time: 34 }]);
  });
});
