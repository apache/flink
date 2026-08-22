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
import { of } from 'rxjs';

import { APP_ICONS } from '@flink-runtime-web/app-icons';
import { JobService } from '@flink-runtime-web/services';
import { provideNzIcons } from 'ng-zorro-antd/icon';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobRescalesComponent } from './job-rescales.component';
import { JobLocalService } from '../job-local.service';

const mockOverview = {
  rescalesCounts: { inProgress: 0, completed: 1, failed: 0, ignored: 0 },
  latest: {}
};

describe('JobRescalesComponent', () => {
  let fixture: ComponentFixture<JobRescalesComponent>;
  let element: HTMLElement;
  const loadRescalesOverview = vi.fn();
  const loadRescalesSummary = vi.fn();
  const loadRescalesHistory = vi.fn();
  const loadRescalesConfig = vi.fn();
  const loadRescaleDetail = vi.fn();

  beforeEach(async () => {
    loadRescalesOverview.mockReset().mockReturnValue(of(mockOverview));
    loadRescalesSummary.mockReset().mockReturnValue(of(undefined));
    loadRescalesHistory.mockReset().mockReturnValue(of(undefined));
    loadRescalesConfig.mockReset().mockReturnValue(of({ maxParallelism: 128 }));
    loadRescaleDetail.mockReset().mockReturnValue(of(undefined));
    await TestBed.configureTestingModule({
      imports: [JobRescalesComponent],
      providers: [
        provideNzIcons(APP_ICONS),
        {
          provide: JobService,
          useValue: {
            loadRescalesOverview,
            loadRescalesSummary,
            loadRescalesHistory,
            loadRescalesConfig,
            loadRescaleDetail
          }
        },
        { provide: JobLocalService, useValue: { jobDetailChanges: () => of({ jid: 'job-1' }) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobRescalesComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads the rescale overview and config for the current job', () => {
    fixture.detectChanges();

    expect(loadRescalesOverview).toHaveBeenCalledWith('job-1');
    expect(loadRescalesConfig).toHaveBeenCalledWith('job-1');
    expect(fixture.componentInstance.rescalesOverview).toEqual(mockOverview);
    expect(fixture.componentInstance.rescalesConfig).toBeDefined();

    // The tab shell renders once the config resolves.
    expect(element.textContent).toContain('Overview');
  });

  it('computes the total rescale count from the overview counts', () => {
    fixture.detectChanges();

    expect(fixture.componentInstance.getTotalRescaleCount()).toBe(1);
  });

  it('truncates uuids and long names for display', () => {
    const component = fixture.componentInstance;

    expect(component.truncateUuid('abcdef1234567890')).toBe('abcdef12');
    expect(component.truncateUuid('')).toBe('');
    expect(component.truncateName('short')).toBe('short');
    expect(component.truncateName('x'.repeat(40))).toBe(`${'x'.repeat(32)}...`);
  });
});
