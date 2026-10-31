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
import { ChangeDetectorRef } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';
import { BehaviorSubject, Observable, of, Subject, throwError } from 'rxjs';

import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JobService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobDetailComponent } from './job-detail.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobDetailComponent', () => {
  const loadJob = vi.fn();
  const loadExceptions = vi.fn();
  const setJobDetail = vi.fn();
  let component: JobDetailComponent;

  function createComponent(
    refresh$: Observable<boolean> = of(true),
    params$: Observable<Params> = of({ jid: 'job-1' }),
    isHistoryServer = false
  ): JobDetailComponent {
    return new JobDetailComponent(
      { loadJob, loadExceptions } as unknown as JobService,
      { setJobDetail } as unknown as JobLocalService,
      { refresh$, configuration: { features: { 'web-history': isHistoryServer } } } as unknown as StatusService,
      { params: params$ } as unknown as ActivatedRoute,
      cdr
    );
  }

  beforeEach(() => {
    loadJob.mockReset();
    loadExceptions.mockReset().mockReturnValue(of({ 'root-exception': '' }));
    setJobDetail.mockReset();
    component = createComponent();
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

  it('stops polling and reports the job as gone once it is no longer found', () => {
    const refresh$ = new Subject<boolean>();
    component = createComponent(refresh$);
    loadJob.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 404 })));

    component.ngOnInit();
    refresh$.next(true);
    refresh$.next(true);
    refresh$.next(true);

    expect(loadJob).toHaveBeenCalledTimes(1);
    expect(component.notFound).toBe(true);
    expect(component.isError).toBe(false);
    expect(component.isLoading).toBe(false);
    expect(loadExceptions).not.toHaveBeenCalled();
  });

  it('polls the new job when the job ID changes after the previous job was not found', () => {
    const refresh$ = new Subject<boolean>();
    const params$ = new BehaviorSubject<Params>({ jid: 'job-1' });
    component = createComponent(refresh$, params$);
    const job = { plan: { jid: 'job-2' } };
    loadJob.mockImplementation((jid: string) =>
      jid === 'job-1' ? throwError(() => new HttpErrorResponse({ status: 404 })) : of(job)
    );

    component.ngOnInit();
    refresh$.next(true);
    params$.next({ jid: 'job-2' });
    expect(component.notFound).toBe(false);
    expect(component.isLoading).toBe(true);

    refresh$.next(true);

    expect(loadJob).toHaveBeenLastCalledWith('job-2');
    expect(setJobDetail).toHaveBeenCalledWith(job);
    expect(component.notFound).toBe(false);
    expect(component.isLoading).toBe(false);
  });

  it('explains a missing job by the History Server archive rather than by expiry', () => {
    expect(createComponent(of(true), of({ jid: 'job-1' }), true).notFoundDescription).toBe(
      'The History Server has no archive for this job.'
    );
    expect(component.notFoundDescription).toContain('jobstore.expiration-time');
  });
});
