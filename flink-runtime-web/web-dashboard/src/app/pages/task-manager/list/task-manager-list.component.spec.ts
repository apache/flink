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
import { ActivatedRoute, Router } from '@angular/router';
import { of, throwError } from 'rxjs';

import { TaskManagersItem } from '@flink-runtime-web/interfaces';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TaskManagerListComponent } from './task-manager-list.component';

const mockTaskManager: TaskManagersItem = {
  id: 'tm-container-42',
  path: 'pekko.tcp://flink@10.0.0.7:6122/user/rpc/taskmanager',
  dataPort: 43210,
  timeSinceLastHeartbeat: 1_781_000_000_000,
  slotsNumber: 4,
  freeSlots: 2,
  assignedTasks: 2,
  hardware: { cpuCores: 8, physicalMemory: 16_000_000_000, freeMemory: 8_000_000_000, managedMemory: 4_000_000_000 },
  blocked: false
};

describe('TaskManagerListComponent', () => {
  let fixture: ComponentFixture<TaskManagerListComponent>;
  let element: HTMLElement;
  const loadManagers = vi.fn();
  const navigate = vi.fn().mockResolvedValue(true);

  beforeEach(async () => {
    loadManagers.mockReset().mockReturnValue(of([mockTaskManager]));
    navigate.mockClear();
    await TestBed.configureTestingModule({
      imports: [TaskManagerListComponent],
      providers: [
        { provide: StatusService, useValue: { refresh$: of(true) } },
        { provide: TaskManagerService, useValue: { loadManagers } },
        { provide: Router, useValue: { navigate } },
        { provide: ActivatedRoute, useValue: {} }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(TaskManagerListComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads task managers on refresh and renders one row per manager', () => {
    fixture.detectChanges();

    expect(loadManagers).toHaveBeenCalled();
    expect(fixture.componentInstance.isLoading).toBe(false);
    expect(fixture.componentInstance.listOfTaskManager).toHaveLength(1);

    // The manager's data-port cell is rendered into the table.
    expect(element.textContent).toContain('43210');
  });

  it('falls back to an empty list when the request fails', () => {
    loadManagers.mockReturnValue(throwError(() => new Error('cluster unreachable')));

    fixture.detectChanges();

    expect(fixture.componentInstance.isLoading).toBe(false);
    expect(fixture.componentInstance.listOfTaskManager).toEqual([]);
    // No manager rows, so the data-port value is absent from the table.
    expect(element.textContent).not.toContain('43210');
  });

  it('navigates to the TaskManager metrics page when a row is clicked', () => {
    fixture.detectChanges();

    element.querySelector<HTMLElement>('tr.clickable')?.click();

    expect(navigate).toHaveBeenCalledWith([mockTaskManager.id, 'metrics'], {
      relativeTo: expect.anything(),
      queryParamsHandling: 'preserve'
    });
  });
});
