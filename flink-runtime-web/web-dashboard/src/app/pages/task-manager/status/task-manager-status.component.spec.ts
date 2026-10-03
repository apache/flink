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

import { TaskManagerStatusComponent } from './task-manager-status.component';

const mockDetail = {
  id: 'tm-container-7',
  path: 'pekko.tcp://flink@10.0.0.7:6122/user/rpc/taskmanager',
  dataPort: 43210,
  timeSinceLastHeartbeat: 1_781_000_000_000,
  slotsNumber: 4,
  freeSlots: 2,
  assignedTasks: 2,
  hardware: { cpuCores: 8, physicalMemory: 16_000_000_000, freeMemory: 8_000_000_000, managedMemory: 4_000_000_000 },
  blocked: false
} as unknown as TaskManagerDetail;

describe('TaskManagerStatusComponent', () => {
  let fixture: ComponentFixture<TaskManagerStatusComponent>;
  let element: HTMLElement;
  const loadManager = vi.fn();

  beforeEach(async () => {
    loadManager.mockReset().mockReturnValue(of(mockDetail));
    await TestBed.configureTestingModule({
      imports: [TaskManagerStatusComponent],
      providers: [
        { provide: StatusService, useValue: { refresh$: of(true) } },
        { provide: TaskManagerService, useValue: { loadManager } },
        { provide: ActivatedRoute, useValue: { snapshot: { params: { taskManagerId: 'tm-container-7' } } } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(TaskManagerStatusComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('renders the TaskManager detail when it is available', () => {
    fixture.detectChanges();

    expect(fixture.componentInstance.notFound).toBe(false);
    expect(element.textContent).toContain('tm-container-7');
    expect(element.textContent).toContain('43210');
    expect(element.textContent).not.toContain('no longer registered');
  });

  it('shows a not-available notice when the TaskManager is gone (404)', () => {
    loadManager.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 404 })));

    fixture.detectChanges();

    expect(fixture.componentInstance.notFound).toBe(true);
    expect(fixture.componentInstance.taskManagerDetail).toBeUndefined();
    expect(element.textContent).toContain('TaskManager not available');
  });
});
