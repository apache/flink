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
import { ActivatedRoute } from '@angular/router';
import { of, throwError } from 'rxjs';

import { TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TaskManagerMetricsComponent } from './task-manager-metrics.component';

const mockDetail = { id: 'tm-container-7' } as unknown as TaskManagerDetail;

describe('TaskManagerMetricsComponent', () => {
  let fixture: ComponentFixture<TaskManagerMetricsComponent>;
  let element: HTMLElement;
  const loadManager = vi.fn();
  const loadMetrics = vi.fn();

  beforeEach(async () => {
    loadManager.mockReset().mockReturnValue(of(mockDetail));
    loadMetrics.mockReset().mockReturnValue(of({ 'Status.JVM.Memory.Heap.Used': 1, 'Status.JVM.Memory.Heap.Max': 2 }));
    await TestBed.configureTestingModule({
      imports: [TaskManagerMetricsComponent],
      providers: [
        { provide: StatusService, useValue: { refresh$: of(true) } },
        { provide: TaskManagerService, useValue: { loadManager, loadMetrics } },
        { provide: ActivatedRoute, useValue: { parent: { snapshot: { params: { taskManagerId: 'tm-container-7' } } } } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(TaskManagerMetricsComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads the metrics when the TaskManager is available', () => {
    // Drive the load path without a full DOM render: the metric cards bind
    // taskManagerDetail.metrics.*, so rendering needs a complete metrics fixture
    // (the production build covers that). Here we assert the wiring.
    fixture.componentInstance.ngOnInit();

    expect(fixture.componentInstance.taskManagerDetail).toBe(mockDetail);
    expect(loadMetrics).toHaveBeenCalled();
  });

  it('hides the metric cards (no NaN) when the TaskManager is gone (404)', () => {
    loadManager.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 404 })));

    fixture.detectChanges();

    expect(fixture.componentInstance.taskManagerDetail).toBeUndefined();
    expect(element.textContent).not.toContain('Flink Memory Model');
    expect(element.textContent).not.toContain('Advanced');
    expect(element.textContent).not.toContain('NaN');
    expect(loadMetrics).not.toHaveBeenCalled();
  });
});
