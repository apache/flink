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

import { JobManagerLogsComponent } from './job-manager-logs.component';

// The log/stdout/thread-dump pages embed a monaco code editor; constructing them directly and
// driving reload() exercises the controller without rendering that editor in jsdom.
const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobManagerLogsComponent', () => {
  const loadLogs = vi.fn();
  let component: JobManagerLogsComponent;

  beforeEach(() => {
    loadLogs.mockReset();
    component = new JobManagerLogsComponent(
      { loadLogs } as unknown as JobManagerService,
      { BASE_URL: '/api' } as unknown as ConfigService,
      cdr,
      JOB_MANAGER_MODULE_DEFAULT_CONFIG
    );
  });

  it('loads the jobmanager log and exposes the download target', () => {
    loadLogs.mockReturnValue(of('2026-01-01 INFO started\n'));

    component.ngOnInit();

    expect(loadLogs).toHaveBeenCalled();
    expect(component.logs).toBe('2026-01-01 INFO started\n');
    expect(component.downloadUrl).toBe('/api/jobmanager/log');
    expect(component.downloadName).toBe('jobmanager_log');
    expect(component.loading).toBe(false);
  });

  it('falls back to empty logs when the request fails', () => {
    loadLogs.mockReturnValue(throwError(() => new Error('log unavailable')));

    component.ngOnInit();

    expect(component.logs).toBe('');
    expect(component.loading).toBe(false);
  });
});
