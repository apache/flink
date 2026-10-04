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

import { JobManagerThreadDumpComponent } from './job-manager-thread-dump.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobManagerThreadDumpComponent', () => {
  const loadThreadDump = vi.fn();
  let component: JobManagerThreadDumpComponent;

  beforeEach(() => {
    loadThreadDump.mockReset().mockReturnValue(of('thread dump\n'));
    component = new JobManagerThreadDumpComponent(
      { loadThreadDump } as unknown as JobManagerService,
      { BASE_URL: '/api' } as unknown as ConfigService,
      cdr,
      JOB_MANAGER_MODULE_DEFAULT_CONFIG
    );
  });

  it('loads the thread dump with no mode by default', () => {
    component.ngOnInit();

    expect(loadThreadDump).toHaveBeenCalledWith(undefined);
    expect(component.dump).toBe('thread dump\n');
    expect(component.downloadUrl).toBe('/api/jobmanager/thread-dump');
    expect(component.loading).toBe(false);
  });

  it('updates mode and download url on selection and reloads with that mode', () => {
    component.selectMode('full');
    expect(component.mode).toBe('full');
    expect(component.downloadUrl).toBe('/api/jobmanager/thread-dump?mode=full');

    component.reload();
    expect(loadThreadDump).toHaveBeenLastCalledWith('full');
  });

  it('ignores re-selecting the current mode', () => {
    component.selectMode('lite');
    component.selectMode('lite');

    expect(component.mode).toBe('lite');
  });

  it('falls back to an empty dump when the request fails', () => {
    loadThreadDump.mockReturnValue(throwError(() => new Error('unavailable')));

    component.ngOnInit();

    expect(component.dump).toBe('');
    expect(component.loading).toBe(false);
  });
});
