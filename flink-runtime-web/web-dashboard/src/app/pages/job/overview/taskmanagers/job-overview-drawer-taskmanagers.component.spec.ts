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

import { JobOverviewDrawerTaskmanagersComponent } from './job-overview-drawer-taskmanagers.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const jobLocalService = {
  jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } })
} as unknown as JobLocalService;

describe('JobOverviewDrawerTaskmanagersComponent', () => {
  const loadTaskManagers = vi.fn();
  let component: JobOverviewDrawerTaskmanagersComponent;

  beforeEach(() => {
    loadTaskManagers.mockReset();
    component = new JobOverviewDrawerTaskmanagersComponent(
      { loadTaskManagers } as unknown as JobService,
      jobLocalService,
      cdr,
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG
    );
  });

  it('extracts the taskmanagers list for the selected vertex', () => {
    loadTaskManagers.mockReturnValue(of({ taskmanagers: [{ endpoint: 'host-a' }, { endpoint: 'host-b' }] }));

    component.ngOnInit();

    expect(loadTaskManagers).toHaveBeenCalledWith('job-1', 'v1');
    expect(component.listOfTaskManager).toEqual([{ endpoint: 'host-a' }, { endpoint: 'host-b' }]);
    expect(component.isLoading).toBe(false);
  });

  it('falls back to an empty list when the request fails', () => {
    loadTaskManagers.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.listOfTaskManager).toEqual([]);
    expect(component.isLoading).toBe(false);
  });
});
