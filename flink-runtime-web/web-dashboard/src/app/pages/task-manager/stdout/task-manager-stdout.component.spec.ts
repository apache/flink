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

import { TASK_MANAGER_MODULE_DEFAULT_CONFIG } from '@flink-runtime-web/pages/task-manager/task-manager.config';
import { ConfigService, TaskManagerService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TaskManagerStdoutComponent } from './task-manager-stdout.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const activatedRoute = { parent: { snapshot: { params: { taskManagerId: 'tm-1' } } } } as unknown as ActivatedRoute;

describe('TaskManagerStdoutComponent', () => {
  const loadStdout = vi.fn();
  let component: TaskManagerStdoutComponent;

  beforeEach(() => {
    loadStdout.mockReset();
    component = new TaskManagerStdoutComponent(
      { loadStdout } as unknown as TaskManagerService,
      { BASE_URL: '/api' } as unknown as ConfigService,
      activatedRoute,
      cdr,
      TASK_MANAGER_MODULE_DEFAULT_CONFIG
    );
  });

  it('loads stdout for the routed task manager and derives the download target', () => {
    loadStdout.mockReturnValue(of('hello stdout\n'));

    component.ngOnInit();

    expect(loadStdout).toHaveBeenCalledWith('tm-1');
    expect(component.taskManagerId).toBe('tm-1');
    expect(component.stdout).toBe('hello stdout\n');
    expect(component.downloadUrl).toBe('/api/taskmanagers/tm-1/stdout');
    expect(component.downloadName).toBe('taskmanager_tm-1_stdout');
    expect(component.loading).toBe(false);
  });

  it('falls back to empty stdout when the request fails', () => {
    loadStdout.mockReturnValue(throwError(() => new Error('unavailable')));

    component.ngOnInit();

    expect(component.stdout).toBe('');
    expect(component.loading).toBe(false);
  });
});
