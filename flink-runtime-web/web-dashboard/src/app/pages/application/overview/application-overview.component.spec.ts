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

import { ApplicationDetail, JobsItem } from '@flink-runtime-web/interfaces';
import { JobService, StatusService } from '@flink-runtime-web/services';
import { NzMessageService } from 'ng-zorro-antd/message';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApplicationOverviewComponent } from './application-overview.component';
import { ApplicationLocalService } from '../application-local.service';

const runningJob = {
  jid: 'job-1',
  name: 'Streaming ETL',
  state: 'RUNNING',
  'start-time': 200,
  'end-time': -1,
  duration: 50,
  completed: false
} as JobsItem;

const finishedJob = {
  jid: 'job-2',
  name: 'Batch Report',
  state: 'FINISHED',
  'start-time': 50,
  'end-time': 150,
  duration: 100,
  completed: true
} as JobsItem;

const mockApplicationDetail = {
  id: 'app-1',
  jobs: [runningJob, finishedJob]
} as ApplicationDetail;

describe('ApplicationOverviewComponent', () => {
  let fixture: ComponentFixture<ApplicationOverviewComponent>;
  let element: HTMLElement;
  const navigate = vi.fn().mockResolvedValue(true);

  beforeEach(async () => {
    navigate.mockClear();
    // JobListComponent (rendered twice by this component) depends on StatusService, JobService
    // and NzMessageService; since jobData$ is passed in directly, they're never actually called.
    await TestBed.configureTestingModule({
      imports: [ApplicationOverviewComponent],
      providers: [
        { provide: ApplicationLocalService, useValue: { applicationDetailChanges: () => of(mockApplicationDetail) } },
        { provide: Router, useValue: { navigate } },
        { provide: StatusService, useValue: {} },
        { provide: JobService, useValue: {} },
        { provide: NzMessageService, useValue: {} }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(ApplicationOverviewComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('splits the application jobs into running and completed lists', () => {
    fixture.detectChanges();

    const [runningList, completedList] = Array.from(element.querySelectorAll('flink-job-list'));
    expect(runningList.textContent).toContain('Running Job List');
    expect(runningList.textContent).toContain('Streaming ETL');
    expect(runningList.textContent).not.toContain('Batch Report');
    expect(completedList.textContent).toContain('Completed Job List');
    expect(completedList.textContent).toContain('Batch Report');
    expect(completedList.textContent).not.toContain('Streaming ETL');
  });

  it('navigates to the selected job', () => {
    fixture.detectChanges();

    fixture.componentInstance.navigateToJob(['job', 'running', 'job-1']);

    expect(navigate).toHaveBeenCalledWith(['job', 'running', 'job-1']);
  });
});
