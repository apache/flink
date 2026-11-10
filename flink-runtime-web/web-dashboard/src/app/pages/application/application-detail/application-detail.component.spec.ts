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

import { ApplicationLocalService } from '@flink-runtime-web/pages/application/application-local.service';
import { ApplicationService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApplicationDetailComponent } from './application-detail.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('ApplicationDetailComponent', () => {
  const loadApplication = vi.fn();
  const setApplicationDetail = vi.fn();
  let component: ApplicationDetailComponent;

  function createComponent(
    refresh$: Observable<boolean> = of(true),
    params$: Observable<Params> = of({ id: 'app-1' }),
    isHistoryServer = false
  ): ApplicationDetailComponent {
    return new ApplicationDetailComponent(
      { loadApplication } as unknown as ApplicationService,
      { setApplicationDetail } as unknown as ApplicationLocalService,
      { refresh$, configuration: { features: { 'web-history': isHistoryServer } } } as unknown as StatusService,
      { params: params$ } as unknown as ActivatedRoute,
      cdr
    );
  }

  beforeEach(() => {
    loadApplication.mockReset();
    setApplicationDetail.mockReset();
    component = createComponent();
  });

  it('publishes the loaded application detail to the local service', () => {
    const application = { id: 'app-1' };
    loadApplication.mockReturnValue(of(application));

    component.ngOnInit();

    expect(loadApplication).toHaveBeenCalledWith('app-1');
    expect(setApplicationDetail).toHaveBeenCalledWith(application);
    expect(component.isLoading).toBe(false);
    expect(component.isError).toBe(false);
  });

  it('flags an error when the application fails to load', () => {
    loadApplication.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.isError).toBe(true);
    expect(component.isLoading).toBe(false);
    expect(setApplicationDetail).not.toHaveBeenCalled();
  });

  it('stops polling and reports the application as gone once it is no longer found', () => {
    const refresh$ = new Subject<boolean>();
    component = createComponent(refresh$);
    loadApplication.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 404 })));

    component.ngOnInit();
    refresh$.next(true);
    refresh$.next(true);
    refresh$.next(true);

    expect(loadApplication).toHaveBeenCalledTimes(1);
    expect(component.notFound).toBe(true);
    expect(component.isError).toBe(false);
    expect(component.isLoading).toBe(false);
  });

  it('polls the new application when the ID changes after the previous one was not found', () => {
    const refresh$ = new Subject<boolean>();
    const params$ = new BehaviorSubject<Params>({ id: 'app-1' });
    component = createComponent(refresh$, params$);
    const application = { id: 'app-2' };
    loadApplication.mockImplementation((id: string) =>
      id === 'app-1' ? throwError(() => new HttpErrorResponse({ status: 404 })) : of(application)
    );

    component.ngOnInit();
    refresh$.next(true);
    params$.next({ id: 'app-2' });
    expect(component.notFound).toBe(false);
    expect(component.isLoading).toBe(true);

    refresh$.next(true);

    expect(loadApplication).toHaveBeenLastCalledWith('app-2');
    expect(setApplicationDetail).toHaveBeenCalledWith(application);
    expect(component.notFound).toBe(false);
    expect(component.isLoading).toBe(false);
  });

  it('explains a missing application by the History Server archive rather than by expiry', () => {
    expect(createComponent(of(true), of({ id: 'app-1' }), true).notFoundDescription).toBe(
      'The History Server has no archive for this application.'
    );
    expect(component.notFoundDescription).toContain('expired');
  });
});
