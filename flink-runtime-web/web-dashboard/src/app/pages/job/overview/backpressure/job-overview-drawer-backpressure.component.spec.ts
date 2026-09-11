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

import { JOB_OVERVIEW_MODULE_DEFAULT_CONFIG } from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerBackpressureComponent } from './job-overview-drawer-backpressure.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const jobLocalService = {
  jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } })
} as unknown as JobLocalService;

describe('JobOverviewDrawerBackpressureComponent', () => {
  const loadOperatorBackPressure = vi.fn();
  const loadSubTasks = vi.fn();
  let component: JobOverviewDrawerBackpressureComponent;

  beforeEach(() => {
    loadOperatorBackPressure.mockReset();
    loadSubTasks.mockReset().mockReturnValue(of({ subtasks: [] }));
    component = new JobOverviewDrawerBackpressureComponent(
      { loadOperatorBackPressure, loadSubTasks } as unknown as JobService,
      jobLocalService,
      cdr,
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG
    );
  });

  it('renders backpressure ratios as rounded percentages and N/A for NaN', () => {
    expect(component.prettyPrint(NaN)).toBe('N/A');
    expect(component.prettyPrint(0.1234)).toBe('12%');
    expect(component.prettyPrint(1)).toBe('100%');
  });

  it('stores the backpressure response and indexes subtasks by index', () => {
    loadOperatorBackPressure.mockReturnValue(of({ status: 'ok', subtasks: [{ subtask: 0, ratio: 0.5 }] }));
    loadSubTasks.mockReturnValue(of({ subtasks: [{ subtask: 0 }, { subtask: 1 }] }));

    component.ngOnInit();

    expect(component.selectedVertex).toEqual({ id: 'v1' });
    expect(component.backpressure).toEqual({ status: 'ok', subtasks: [{ subtask: 0, ratio: 0.5 }] });
    expect(component.listOfSubTaskBackpressure).toEqual([{ subtask: 0, ratio: 0.5 }]);
    expect(component.mapOfSubtask.get(1)).toEqual({ subtask: 1 });
    expect(component.isLoading).toBe(false);
  });

  it('falls back to an empty backpressure view when the request fails', () => {
    loadOperatorBackPressure.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.backpressure).toEqual({});
    expect(component.listOfSubTaskBackpressure).toEqual([]);
    expect(component.isLoading).toBe(false);
  });
});
