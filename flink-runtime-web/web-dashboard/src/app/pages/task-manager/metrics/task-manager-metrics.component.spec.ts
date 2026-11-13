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

import { HttpErrorResponse } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, ParamMap } from '@angular/router';
import { BehaviorSubject, of, Subject, throwError } from 'rxjs';

import { MetricMap, TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TaskManagerMetricsComponent } from './task-manager-metrics.component';

const mockDetail: TaskManagerDetail = {
  id: 'tm-1',
  path: 'pekko.tcp://flink@localhost:6122/user/rpc/taskmanager_0',
  dataPort: 6121,
  timeSinceLastHeartbeat: 1_700_000_000_000,
  slotsNumber: 4,
  freeSlots: 4,
  assignedTasks: 0,
  hardware: { cpuCores: 8, physicalMemory: 17_179_869_184, freeMemory: 536_870_912, managedMemory: 536_870_912 },
  memoryConfiguration: {
    frameworkHeap: 134_217_728,
    frameworkOffHeap: 134_217_728,
    jvmMetaspace: 268_435_456,
    jvmOverhead: 201_326_592,
    managedMemory: 536_870_912,
    networkMemory: 134_217_728,
    taskHeap: 402_653_184,
    taskOffHeap: 0,
    totalFlinkMemory: 1_476_395_008,
    totalProcessMemory: 1_728_053_248
  },
  metrics: {
    heapUsed: 100,
    heapCommitted: 200,
    heapMax: 400,
    nonHeapUsed: 40,
    nonHeapCommitted: 60,
    nonHeapMax: 300,
    directCount: 5,
    directUsed: 10,
    directMax: 10,
    mappedCount: 0,
    mappedUsed: 0,
    mappedMax: 0,
    nettyShuffleMemorySegmentsAvailable: 1234,
    nettyShuffleMemorySegmentsTotal: 5678,
    garbageCollectors: []
  },
  freeResource: { cpuCores: 4, taskHeapMemory: 100, taskOffHeapMemory: 0, managedMemory: 200, networkMemory: 50 },
  totalResource: { cpuCores: 8, taskHeapMemory: 400, taskOffHeapMemory: 0, managedMemory: 500, networkMemory: 100 },
  allocatedSlots: []
};

const mockMetrics = {
  'Status.JVM.Memory.Heap.Used': 100,
  'Status.JVM.Memory.Heap.Max': 400,
  'Status.Shuffle.Netty.UsedMemory': 10,
  'Status.Shuffle.Netty.TotalMemory': 100,
  'Status.Flink.Memory.Managed.Used': 200,
  'Status.Flink.Memory.Managed.Total': 500,
  'Status.JVM.Memory.Metaspace.Used': 50,
  'Status.JVM.Memory.Metaspace.Max': 250
};

describe('TaskManagerMetricsComponent', () => {
  let fixture: ComponentFixture<TaskManagerMetricsComponent>;
  let element: HTMLElement;
  let paramMap: BehaviorSubject<ParamMap>;
  const loadManager = vi.fn();
  const loadMetrics = vi.fn();

  beforeEach(async () => {
    paramMap = new BehaviorSubject(convertToParamMap({ taskManagerId: 'tm-1' }));
    loadManager.mockReset().mockReturnValue(of(mockDetail));
    loadMetrics.mockReset().mockReturnValue(of(mockMetrics));
    await TestBed.configureTestingModule({
      imports: [TaskManagerMetricsComponent],
      providers: [
        { provide: TaskManagerService, useValue: { loadManager, loadMetrics } },
        { provide: StatusService, useValue: { refresh$: of(true) } },
        {
          provide: ActivatedRoute,
          useValue: { parent: { paramMap, snapshot: { params: { taskManagerId: 'tm-1' } } } }
        }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(TaskManagerMetricsComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads the manager detail and memory metrics on refresh', () => {
    fixture.detectChanges();

    expect(loadManager).toHaveBeenCalledWith('tm-1');
    expect(loadMetrics).toHaveBeenCalledWith('tm-1', expect.arrayContaining(['Status.JVM.Memory.Heap.Used']));
    expect(fixture.componentInstance.taskManagerDetail).toEqual(mockDetail);
    expect(fixture.componentInstance.metrics).toEqual(mockMetrics);

    const text = element.textContent ?? '';
    expect(text).toContain('Memory');
    expect(text).toContain('Flink Memory Model');
  });

  it('shows the Netty shuffle buffer counts', () => {
    fixture.detectChanges();

    const table = Array.from(element.querySelectorAll('nz-table')).find(t =>
      t.textContent?.includes('Netty Shuffle Buffers')
    );
    const cells = Array.from(table?.querySelectorAll('tbody td') ?? []).map(td => td.textContent?.trim());
    expect(cells).toEqual(['Available', '1,234', 'Total', '5,678']);
  });

  it('does not load metrics when the manager cannot be fetched', () => {
    loadManager.mockReturnValue(throwError(() => new Error('gone')));

    fixture.detectChanges();

    expect(fixture.componentInstance.taskManagerDetail).toBeUndefined();
    expect(loadMetrics).not.toHaveBeenCalled();
  });

  it('hides the metric cards (no NaN) when the TaskManager is gone (404)', () => {
    loadManager.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 404 })));

    fixture.detectChanges();

    expect(fixture.componentInstance.taskManagerDetail).toBeUndefined();
    expect(loadMetrics).not.toHaveBeenCalled();
    const text = element.textContent ?? '';
    expect(text).not.toContain('Flink Memory Model');
    expect(text).not.toContain('Advanced');
    expect(text).not.toContain('NaN');
  });

  it('loads metrics for another TaskManager after the previous one returned 404', () => {
    loadManager.mockReturnValueOnce(throwError(() => new HttpErrorResponse({ status: 404 })));
    fixture.detectChanges();
    expect(loadMetrics).not.toHaveBeenCalled();

    loadManager.mockReturnValue(of({ ...mockDetail, id: 'tm-2' }));
    paramMap.next(convertToParamMap({ taskManagerId: 'tm-2' }));
    fixture.detectChanges();

    expect(loadManager).toHaveBeenLastCalledWith('tm-2');
    expect(loadMetrics).toHaveBeenCalledWith('tm-2', expect.arrayContaining(['Status.JVM.Memory.Heap.Used']));
    expect(fixture.componentInstance.taskManagerDetail?.id).toBe('tm-2');
    expect(element.textContent).toContain('Flink Memory Model');
  });

  it('clears stale detail and metrics while the next TaskManager is loading', () => {
    fixture.detectChanges();
    loadManager.mockReturnValue(new Subject<TaskManagerDetail>());

    paramMap.next(convertToParamMap({ taskManagerId: 'tm-2' }));
    fixture.detectChanges();

    expect(fixture.componentInstance.taskManagerDetail).toBeUndefined();
    expect(fixture.componentInstance.metrics).toEqual({});
    expect(element.textContent).not.toContain('Flink Memory Model');
  });

  it('cancels metrics requests for the previous TaskManager when the route changes', () => {
    const previousMetrics = new Subject<MetricMap>();
    loadMetrics.mockReturnValueOnce(previousMetrics);
    fixture.detectChanges();

    loadManager.mockReturnValue(of({ ...mockDetail, id: 'tm-2' }));
    paramMap.next(convertToParamMap({ taskManagerId: 'tm-2' }));
    previousMetrics.next({ 'Status.JVM.Memory.Heap.Used': 999 });
    fixture.detectChanges();

    expect(fixture.componentInstance.taskManagerDetail?.id).toBe('tm-2');
    expect(fixture.componentInstance.metrics).toEqual(mockMetrics);
  });
});
