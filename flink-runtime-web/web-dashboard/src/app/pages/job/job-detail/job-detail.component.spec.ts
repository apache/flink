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
import { ActivatedRoute } from '@angular/router';
import { of, throwError } from 'rxjs';

import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JobService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobDetailComponent } from './job-detail.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const activatedRoute = { snapshot: { params: { jid: 'job-1' } } } as unknown as ActivatedRoute;

describe('JobDetailComponent', () => {
  const loadJob = vi.fn();
  const loadExceptions = vi.fn();
  const setJobDetail = vi.fn();
  let component: JobDetailComponent;

  beforeEach(() => {
    loadJob.mockReset();
    loadExceptions.mockReset().mockReturnValue(of({ 'root-exception': '' }));
    setJobDetail.mockReset();
    component = new JobDetailComponent(
      { loadJob, loadExceptions } as unknown as JobService,
      { setJobDetail } as unknown as JobLocalService,
      { refresh$: of(true) } as unknown as StatusService,
      activatedRoute,
      cdr
    );
  });

  it('publishes the loaded job detail to the local service', () => {
    const job = { plan: { jid: 'job-1' } };
    loadJob.mockReturnValue(of(job));

    component.ngOnInit();

    expect(loadJob).toHaveBeenCalledWith('job-1');
    expect(setJobDetail).toHaveBeenCalledWith(job);
    expect(component.isLoading).toBe(false);
    expect(component.isError).toBe(false);
  });

  it('surfaces the root exception when the job fails to load', () => {
    loadJob.mockReturnValue(throwError(() => new Error('boom')));
    loadExceptions.mockReturnValue(of({ 'root-exception': 'java.lang.RuntimeException: boom' }));

    component.ngOnInit();

    expect(component.isError).toBe(true);
    expect(component.isLoading).toBe(false);
    expect(loadExceptions).toHaveBeenCalledWith('job-1', 10);
    expect(component.errorDetails).toBe('java.lang.RuntimeException: boom');
  });
});
