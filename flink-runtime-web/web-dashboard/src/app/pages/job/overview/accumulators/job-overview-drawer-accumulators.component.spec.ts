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

import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerAccumulatorsComponent } from './job-overview-drawer-accumulators.component';
import { JobLocalService } from '../../job-local.service';

// The drawer components pull their data through services in ngOnInit and, when rendered, drag in
// chart/dynamic-host children jsdom cannot draw. These specs construct the component directly and
// assert the controller logic, rather than rendering the template through TestBed.
const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const jobLocalService = {
  jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } })
} as unknown as JobLocalService;

describe('JobOverviewDrawerAccumulatorsComponent', () => {
  const loadAccumulators = vi.fn();
  let component: JobOverviewDrawerAccumulatorsComponent;

  beforeEach(() => {
    loadAccumulators.mockReset();
    component = new JobOverviewDrawerAccumulatorsComponent(
      { loadAccumulators } as unknown as JobService,
      jobLocalService,
      cdr
    );
  });

  it('flattens the per-subtask user accumulators into individual rows', () => {
    loadAccumulators.mockReturnValue(
      of({
        main: [{ name: 'records', type: 'Long', value: '5' }],
        subtasks: [{ subtask: 0, 'user-accumulators': [{ name: 'records', type: 'Long', value: '5' }] }]
      })
    );

    component.ngOnInit();

    expect(loadAccumulators).toHaveBeenCalledWith('job-1', 'v1');
    expect(component.listOfAccumulator).toEqual([{ name: 'records', type: 'Long', value: '5' }]);
    expect(component.listOfSubTaskAccumulator).toHaveLength(1);
    expect(component.listOfSubTaskAccumulator[0]).toMatchObject({
      subtask: 0,
      name: 'records',
      type: 'Long',
      value: '5'
    });
    expect(component.isLoading).toBe(false);
  });

  it('falls back to empty tables when the accumulators request fails', () => {
    loadAccumulators.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.listOfAccumulator).toEqual([]);
    expect(component.listOfSubTaskAccumulator).toEqual([]);
    expect(component.isLoading).toBe(false);
  });
});
