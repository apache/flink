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

import { JOB_MANAGER_MODULE_DEFAULT_CONFIG } from '@flink-runtime-web/pages/job-manager/job-manager.config';
import { ConfigService, JobManagerService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobManagerStdoutComponent } from './job-manager-stdout.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobManagerStdoutComponent', () => {
  const loadStdout = vi.fn();
  let component: JobManagerStdoutComponent;

  beforeEach(() => {
    loadStdout.mockReset();
    component = new JobManagerStdoutComponent(
      { loadStdout } as unknown as JobManagerService,
      { BASE_URL: '/api' } as unknown as ConfigService,
      cdr,
      JOB_MANAGER_MODULE_DEFAULT_CONFIG
    );
  });

  it('loads the jobmanager stdout and exposes the download target', () => {
    loadStdout.mockReturnValue(of('hello stdout\n'));

    component.ngOnInit();

    expect(loadStdout).toHaveBeenCalled();
    expect(component.stdout).toBe('hello stdout\n');
    expect(component.downloadUrl).toBe('/api/jobmanager/stdout');
    expect(component.downloadName).toBe('jobmanager_stdout');
    expect(component.loading).toBe(false);
  });

  it('falls back to empty stdout when the request fails', () => {
    loadStdout.mockReturnValue(throwError(() => new Error('stdout unavailable')));

    component.ngOnInit();

    expect(component.stdout).toBe('');
    expect(component.loading).toBe(false);
  });
});
