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
import { of } from 'rxjs';

import { ProfilingDetail } from '@flink-runtime-web/interfaces/job-profiler';
import { JobManagerService, StatusService } from '@flink-runtime-web/services';
import { NzMessageService } from 'ng-zorro-antd/message';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobManagerProfilerComponent } from './job-manager-profiler.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobManagerProfilerComponent', () => {
  const loadProfilingList = vi.fn();
  const createProfilingInstance = vi.fn();
  const warning = vi.fn();
  let component: JobManagerProfilerComponent;

  beforeEach(() => {
    loadProfilingList.mockReset().mockReturnValue(of({ profilingList: [] }));
    createProfilingInstance.mockReset();
    warning.mockReset();
    component = new JobManagerProfilerComponent(
      { loadProfilingList, createProfilingInstance } as unknown as JobManagerService,
      { refresh$: of(true) } as unknown as StatusService,
      { warning } as unknown as NzMessageService,
      cdr
    );
  });

  it('loads the profiling list and marks profiling enabled on refresh', () => {
    loadProfilingList.mockReturnValue(of({ profilingList: [{ status: 'FINISHED' } as ProfilingDetail] }));

    component.ngOnInit();

    expect(component.profilingList).toEqual([{ status: 'FINISHED' }]);
    expect(component.isEnabled).toBe(true);
    expect(component.isLoading).toBe(false);
  });

  it('prepends a newly created profiling instance', () => {
    createProfilingInstance.mockReturnValue(of({ status: 'RUNNING' } as ProfilingDetail));

    component.createProfilingInstance();

    expect(createProfilingInstance).toHaveBeenCalledWith('ITIMER', 30);
    expect(component.profilingList[0]).toEqual({ status: 'RUNNING' });
    expect(component.isCreating).toBe(false);
  });

  it('refuses to start a new profiling run while one is still running', () => {
    component.profilingList = [{ status: 'RUNNING' } as ProfilingDetail];

    component.createProfilingInstance();

    expect(warning).toHaveBeenCalled();
    expect(createProfilingInstance).not.toHaveBeenCalled();
  });
});
