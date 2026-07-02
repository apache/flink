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

import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobExceptionsComponent } from './job-exceptions.component';
import { JobLocalService } from '../job-local.service';

const rootException = {
  exceptionName: 'java.lang.RuntimeException',
  stacktrace: 'java.lang.RuntimeException: boom\n\tat com.example.Foo.bar(Foo.java:42)',
  timestamp: 1_781_000_000_000,
  failureLabels: {},
  taskName: 'Source: myGenerator (1/2)',
  endpoint: 'localhost:43210',
  taskManagerId: 'tm-1',
  concurrentExceptions: []
};

const mockExceptions = {
  exceptionHistory: { entries: [rootException], truncated: false }
};

describe('JobExceptionsComponent', () => {
  let fixture: ComponentFixture<JobExceptionsComponent>;
  let element: HTMLElement;
  const loadExceptions = vi.fn();
  const navigate = vi.fn().mockResolvedValue(true);

  beforeEach(async () => {
    loadExceptions.mockReset().mockReturnValue(of(mockExceptions));
    navigate.mockClear();
    await TestBed.configureTestingModule({
      imports: [JobExceptionsComponent],
      providers: [
        { provide: JobService, useValue: { loadExceptions } },
        { provide: JobLocalService, useValue: { jobDetailChanges: () => of({ jid: 'job-1' }) } },
        { provide: Router, useValue: { navigate } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobExceptionsComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads and maps the exception history for the current job', () => {
    fixture.detectChanges();

    expect(loadExceptions).toHaveBeenCalledWith('job-1', 10);
    expect(fixture.componentInstance.exceptionHistory).toHaveLength(1);
    expect(fixture.componentInstance.exceptionHistory[0].selected.exceptionName).toBe('java.lang.RuntimeException');
    expect(fixture.componentInstance.rootException).toContain('java.lang.RuntimeException: boom');

    // The ng-zorro tabbed shell renders (history table lives in the lazy second tab).
    expect(element.textContent).toContain('Root Exception');
    expect(element.textContent).toContain('Exception History');
  });

  it('navigates to the TaskManager metrics for a given exception entry', () => {
    fixture.detectChanges();

    fixture.componentInstance.navigateTo('tm-1');

    expect(navigate).toHaveBeenCalledWith(['task-manager', 'tm-1', 'metrics']);
  });

  it('skips navigation when no TaskManager is assigned', () => {
    fixture.detectChanges();

    fixture.componentInstance.navigateTo(undefined);

    expect(navigate).not.toHaveBeenCalled();
  });
});
