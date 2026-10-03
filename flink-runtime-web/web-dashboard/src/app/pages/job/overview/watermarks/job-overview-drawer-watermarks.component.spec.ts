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

import { MetricsService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerWatermarksComponent } from './job-overview-drawer-watermarks.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const jobLocalService = {
  jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } })
} as unknown as JobLocalService;

describe('JobOverviewDrawerWatermarksComponent', () => {
  const loadWatermarks = vi.fn();
  let component: JobOverviewDrawerWatermarksComponent;

  beforeEach(() => {
    loadWatermarks.mockReset();
    component = new JobOverviewDrawerWatermarksComponent(
      jobLocalService,
      { loadWatermarks } as unknown as MetricsService,
      cdr
    );
  });

  it('turns the watermark map into indexed rows', () => {
    loadWatermarks.mockReturnValue(of({ watermarks: { '0': 100, '1': 200 } }));

    component.ngOnInit();

    expect(loadWatermarks).toHaveBeenCalledWith('job-1', 'v1');
    expect(component.listOfWaterMark).toEqual([
      { subTaskIndex: 0, watermark: 100 },
      { subTaskIndex: 1, watermark: 200 }
    ]);
    expect(component.isLoading).toBe(false);
  });

  it('falls back to an empty table when the watermark request fails', () => {
    loadWatermarks.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.listOfWaterMark).toEqual([]);
    expect(component.isLoading).toBe(false);
  });

  it('sorts watermarks ascending', () => {
    expect(component.sortWatermark({ subTaskIndex: 0, watermark: 5 }, { subTaskIndex: 1, watermark: 3 })).toBe(2);
  });
});
