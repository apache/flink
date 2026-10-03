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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { of, throwError } from 'rxjs';

import { APP_ICONS } from '@flink-runtime-web/app-icons';
import { TASK_MANAGER_MODULE_CONFIG } from '@flink-runtime-web/pages/task-manager/task-manager.config';
import { ConfigService, TaskManagerService } from '@flink-runtime-web/services';
import { provideNzIcons } from 'ng-zorro-antd/icon';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TaskManagerLogsComponent } from './task-manager-logs.component';

describe('TaskManagerLogsComponent', () => {
  let fixture: ComponentFixture<TaskManagerLogsComponent>;
  const loadLogs = vi.fn();

  beforeEach(async () => {
    loadLogs.mockReset().mockReturnValue(of('2026-01-01 INFO started\n'));
    await TestBed.configureTestingModule({
      imports: [TaskManagerLogsComponent],
      providers: [
        provideNzIcons(APP_ICONS),
        { provide: TaskManagerService, useValue: { loadLogs } },
        { provide: ConfigService, useValue: { BASE_URL: '/api' } },
        { provide: ActivatedRoute, useValue: { parent: { snapshot: { params: { taskManagerId: 'tm-1' } } } } },
        { provide: TASK_MANAGER_MODULE_CONFIG, useValue: {} }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(TaskManagerLogsComponent);
  });

  it('loads logs for the task manager and derives the download target', () => {
    fixture.detectChanges();

    expect(loadLogs).toHaveBeenCalledWith('tm-1');
    expect(fixture.componentInstance.taskManagerId).toBe('tm-1');
    expect(fixture.componentInstance.downloadUrl).toBe('/api/taskmanagers/tm-1/log');
    expect(fixture.componentInstance.downloadName).toBe('taskmanager_tm-1_log');
    expect(fixture.componentInstance.logs).toBe('2026-01-01 INFO started\n');
    expect(fixture.componentInstance.loading).toBe(false);
  });

  it('falls back to empty logs when the request fails', () => {
    loadLogs.mockReturnValue(throwError(() => new Error('log unavailable')));

    fixture.detectChanges();

    expect(fixture.componentInstance.logs).toBe('');
    expect(fixture.componentInstance.loading).toBe(false);
  });
});
