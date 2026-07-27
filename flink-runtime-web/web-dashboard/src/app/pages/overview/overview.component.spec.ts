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
import { Router } from '@angular/router';
import { of } from 'rxjs';

import { ApplicationService, OverviewService, StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { OverviewComponent } from './overview.component';

const emptyJobStatus = {
  CANCELED: 0,
  CANCELING: 0,
  CREATED: 0,
  FAILED: 0,
  FAILING: 0,
  FINISHED: 0,
  RECONCILING: 0,
  RUNNING: 0,
  RESTARTING: 0
};

const mockApplications = [
  {
    id: 'app-1',
    name: 'running-app',
    status: 'RUNNING',
    'start-time': 100,
    'end-time': -1,
    duration: 10,
    completed: false,
    jobs: { ...emptyJobStatus, RUNNING: 1 }
  },
  {
    id: 'app-2',
    name: 'finished-app',
    status: 'FINISHED',
    'start-time': 50,
    'end-time': 200,
    duration: 150,
    completed: true,
    jobs: { ...emptyJobStatus, FINISHED: 1 }
  }
];

const mockClusterOverview = {
  taskmanagers: 3,
  'taskmanagers-blocked': 0,
  'slots-total': 12,
  'slots-available': 5,
  'slots-free-and-blocked': 0,
  'flink-version': '2.4-SNAPSHOT',
  'flink-commit': 'abcdef0'
};

describe('OverviewComponent', () => {
  let fixture: ComponentFixture<OverviewComponent>;
  let element: HTMLElement;
  const loadApplications = vi.fn();
  const loadOverview = vi.fn();
  const navigate = vi.fn().mockResolvedValue(true);

  beforeEach(async () => {
    loadApplications.mockReset().mockReturnValue(of(mockApplications));
    loadOverview.mockReset().mockReturnValue(of(mockClusterOverview));
    navigate.mockClear();
    await TestBed.configureTestingModule({
      imports: [OverviewComponent],
      providers: [
        { provide: StatusService, useValue: { refresh$: of(true) } },
        { provide: ApplicationService, useValue: { loadApplications } },
        { provide: OverviewService, useValue: { loadOverview } },
        { provide: Router, useValue: { navigate } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(OverviewComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('composes the statistic card and application lists from cluster data', () => {
    fixture.detectChanges();

    expect(loadApplications).toHaveBeenCalled();
    expect(loadOverview).toHaveBeenCalled();

    const text = element.textContent ?? '';
    // Statistic card (child component) is populated from the derived stats.
    expect(text).toContain('Available Task Slots');
    // Both application-list children render with their titles.
    expect(text).toContain('Running Application List');
    expect(text).toContain('Completed Application List');
  });

  it('navigates when an application is selected', () => {
    fixture.detectChanges();

    fixture.componentInstance.navigateToApplication(['application', 'running', 'app-1']);

    expect(navigate).toHaveBeenCalledWith(['application', 'running', 'app-1']);
  });
});
