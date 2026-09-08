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

import { TaskManagerThreadDumpComponent } from './task-manager-thread-dump.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const activatedRoute = {
  parent: { snapshot: { params: { taskManagerId: 'tm-1' } } },
  queryParams: of({ vertexName: 'Source%3A%20generator' })
} as unknown as ActivatedRoute;

describe('TaskManagerThreadDumpComponent', () => {
  const loadThreadDump = vi.fn();
  let component: TaskManagerThreadDumpComponent;

  beforeEach(() => {
    loadThreadDump.mockReset().mockReturnValue(of('thread dump\n'));
    component = new TaskManagerThreadDumpComponent(
      { loadThreadDump } as unknown as TaskManagerService,
      { BASE_URL: '/api' } as unknown as ConfigService,
      activatedRoute,
      cdr,
      TASK_MANAGER_MODULE_DEFAULT_CONFIG
    );
  });

  it('derives ids and download target and decodes the vertex name on init', () => {
    component.ngOnInit();

    expect(component.taskManagerId).toBe('tm-1');
    expect(component.downloadName).toBe('taskmanager_tm-1_thread_dump');
    expect(component.downloadUrl).toBe('/api/taskmanagers/tm-1/thread-dump');
    expect(component.vertexName).toBe('Source: generator');
  });

  it('loads the dump with the current mode via reload', () => {
    component.ngOnInit();

    component.reload();

    expect(loadThreadDump).toHaveBeenCalledWith('tm-1', undefined);
    expect(component.dump).toBe('thread dump\n');
    expect(component.loading).toBe(false);
  });

  it('updates mode and download url on selection and reloads with that mode', () => {
    component.ngOnInit();

    component.selectMode('full');
    expect(component.mode).toBe('full');
    expect(component.downloadUrl).toBe('/api/taskmanagers/tm-1/thread-dump?mode=full');

    component.reload();
    expect(loadThreadDump).toHaveBeenLastCalledWith('tm-1', 'full');
  });

  it('falls back to an empty dump when the request fails', () => {
    loadThreadDump.mockReturnValue(throwError(() => new Error('unavailable')));
    component.ngOnInit();

    component.reload();

    expect(component.dump).toBe('');
    expect(component.loading).toBe(false);
  });
});
