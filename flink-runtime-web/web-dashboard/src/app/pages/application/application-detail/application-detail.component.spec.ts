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

import { ApplicationLocalService } from '@flink-runtime-web/pages/application/application-local.service';
import { ApplicationService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApplicationDetailComponent } from './application-detail.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const activatedRoute = { snapshot: { params: { id: 'app-1' } } } as unknown as ActivatedRoute;

describe('ApplicationDetailComponent', () => {
  const loadApplication = vi.fn();
  const setApplicationDetail = vi.fn();
  let component: ApplicationDetailComponent;

  beforeEach(() => {
    loadApplication.mockReset();
    setApplicationDetail.mockReset();
    component = new ApplicationDetailComponent(
      { loadApplication } as unknown as ApplicationService,
      { setApplicationDetail } as unknown as ApplicationLocalService,
      { refresh$: of(true) } as unknown as StatusService,
      activatedRoute,
      cdr
    );
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
});
