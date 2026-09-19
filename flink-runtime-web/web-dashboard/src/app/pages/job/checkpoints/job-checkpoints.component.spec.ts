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
import { of, throwError } from 'rxjs';

import { APP_ICONS } from '@flink-runtime-web/app-icons';
import { JobService } from '@flink-runtime-web/services';
import { provideNzIcons } from 'ng-zorro-antd/icon';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobCheckpointsComponent } from './job-checkpoints.component';
import { JobLocalService } from '../job-local.service';

const mockStats = {
  counts: { restored: 0, total: 3, in_progress: 0, completed: 3, failed: 0 },
  summary: null,
  latest: {},
  history: []
};

const mockConfig = {
  mode: 'exactly_once',
  checkpoint_storage: 'FileSystemCheckpointStorage',
  state_backend: 'HashMapStateBackend',
  interval: 10_000,
  timeout: 600_000,
  min_pause: 0,
  max_concurrent: 1,
  unaligned_checkpoints: false,
  externalization: { enabled: false, delete_on_cancellation: false },
  tolerable_failed_checkpoints: 0,
  checkpoints_after_tasks_finish: false,
  state_changelog_enabled: false
};

describe('JobCheckpointsComponent', () => {
  let fixture: ComponentFixture<JobCheckpointsComponent>;
  let element: HTMLElement;
  const loadCheckpointStats = vi.fn();
  const loadCheckpointConfig = vi.fn();

  beforeEach(async () => {
    loadCheckpointStats.mockReset().mockReturnValue(of(mockStats));
    loadCheckpointConfig.mockReset().mockReturnValue(of(mockConfig));
    await TestBed.configureTestingModule({
      imports: [JobCheckpointsComponent],
      providers: [
        provideNzIcons(APP_ICONS),
        { provide: JobService, useValue: { loadCheckpointStats, loadCheckpointConfig } },
        { provide: JobLocalService, useValue: { jobDetailChanges: () => of({ jid: 'job-1' }) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobCheckpointsComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads checkpoint stats and config for the current job', () => {
    fixture.detectChanges();

    expect(loadCheckpointStats).toHaveBeenCalledWith('job-1');
    expect(loadCheckpointConfig).toHaveBeenCalledWith('job-1');
    expect(fixture.componentInstance.checkPointStats).toEqual(mockStats);
    expect(fixture.componentInstance.checkPointConfig).toEqual(mockConfig);

    // The tabbed shell and the overview counts render once stats are present.
    expect(element.textContent).toContain('Overview');
    expect(element.textContent).toContain('Checkpoint Counts');
  });

  it('shows the empty state when the stats request fails', () => {
    loadCheckpointStats.mockReturnValue(throwError(() => new Error('unreachable')));

    fixture.detectChanges();

    expect(fixture.componentInstance.checkPointStats).toBeUndefined();
    expect(element.querySelector('nz-empty')).not.toBeNull();
  });
});
