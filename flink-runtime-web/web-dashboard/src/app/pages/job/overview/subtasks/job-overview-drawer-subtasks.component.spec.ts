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

import { ChangeDetectorRef } from '@angular/core';
import { of, throwError } from 'rxjs';

import { JobVertexStatusDuration } from '@flink-runtime-web/interfaces';
import { JOB_OVERVIEW_MODULE_DEFAULT_CONFIG } from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerSubtasksComponent } from './job-overview-drawer-subtasks.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const jobLocalService = {
  jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } })
} as unknown as JobLocalService;

describe('JobOverviewDrawerSubtasksComponent', () => {
  const loadSubTasks = vi.fn();
  let component: JobOverviewDrawerSubtasksComponent;

  beforeEach(() => {
    loadSubTasks.mockReset();
    component = new JobOverviewDrawerSubtasksComponent(
      { loadSubTasks } as unknown as JobService,
      jobLocalService,
      cdr,
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG
    );
  });

  it('loads the subtasks and aggregated metrics for the selected vertex', () => {
    loadSubTasks.mockReturnValue(of({ subtasks: [{ subtask: 0 }], aggregated: { 'read-bytes': { min: 1 } } }));

    component.ngOnInit();

    expect(loadSubTasks).toHaveBeenCalledWith('job-1', 'v1');
    expect(component.listOfTask).toEqual([{ subtask: 0 }]);
    expect(component.aggregated).toEqual({ 'read-bytes': { min: 1 } });
    expect(component.isLoading).toBe(false);
  });

  it('falls back to an empty task list when the request fails', () => {
    loadSubTasks.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.listOfTask).toEqual([]);
    expect(component.aggregated).toBeUndefined();
    expect(component.isLoading).toBe(false);
  });

  it('orders the status durations by lifecycle stage', () => {
    const statusDuration = {
      CREATED: 10,
      SCHEDULED: 20,
      DEPLOYING: 30,
      INITIALIZING: 40,
      RUNNING: 50
    } as unknown as JobVertexStatusDuration<number>;

    expect(component.convertStatusDuration(statusDuration)).toEqual([
      { state: 'CREATED', duration: 10 },
      { state: 'SCHEDULED', duration: 20 },
      { state: 'DEPLOYING', duration: 30 },
      { state: 'INITIALIZING', duration: 40 },
      { state: 'RUNNING', duration: 50 }
    ]);
  });
});
