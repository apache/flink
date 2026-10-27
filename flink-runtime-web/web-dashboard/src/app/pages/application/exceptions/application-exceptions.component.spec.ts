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

import { ApplicationDetail, ApplicationExceptions } from '@flink-runtime-web/interfaces';
import { ApplicationService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApplicationExceptionsComponent } from './application-exceptions.component';
import { ApplicationLocalService } from '../application-local.service';

const mockApplicationDetail = { id: 'app-1' } as ApplicationDetail;

describe('ApplicationExceptionsComponent', () => {
  let fixture: ComponentFixture<ApplicationExceptionsComponent>;
  const loadExceptions = vi.fn();

  beforeEach(async () => {
    loadExceptions.mockReset();
    await TestBed.configureTestingModule({
      imports: [ApplicationExceptionsComponent],
      providers: [
        { provide: ApplicationService, useValue: { loadExceptions } },
        { provide: ApplicationLocalService, useValue: { applicationDetailChanges: () => of(mockApplicationDetail) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(ApplicationExceptionsComponent);
  });

  it('formats the most recent exception with its timestamp and related job', () => {
    const mockExceptions: ApplicationExceptions = {
      exceptionHistory: {
        entries: [
          {
            exceptionName: 'java.lang.RuntimeException',
            stacktrace: 'java.lang.RuntimeException: boom\n\tat com.example.Foo.bar(Foo.java:42)',
            timestamp: 1_781_000_000_000,
            jobId: 'job-1'
          },
          {
            exceptionName: 'java.lang.IllegalStateException',
            stacktrace: 'java.lang.IllegalStateException: stale\n\tat com.example.Bar.baz(Bar.java:7)',
            timestamp: 1_780_000_000_000,
            jobId: 'job-0'
          }
        ]
      }
    };
    loadExceptions.mockReturnValue(of(mockExceptions));

    fixture.detectChanges();

    expect(loadExceptions).toHaveBeenCalledWith('app-1');
    const rootException = fixture.componentInstance.rootException;
    // The first line is the formatted timestamp; assert its shape rather than a value, since
    // formatDate is called here without a time zone so the rendered value is environment-dependent.
    expect(rootException.split('\n')[0]).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
    // Only the most recent (first) entry is rendered, not the older one below it.
    expect(rootException).toContain('Related Job: job-1');
    expect(rootException).toContain('java.lang.RuntimeException: boom');
    expect(rootException).not.toContain('job-0');
    expect(rootException).not.toContain('IllegalStateException');
    expect(fixture.componentInstance.isLoading).toBe(false);
  });

  it('omits the related job line when the exception has no job id', () => {
    loadExceptions.mockReturnValue(
      of({
        exceptionHistory: {
          entries: [{ exceptionName: 'java.lang.RuntimeException', stacktrace: 'boom', timestamp: 0 }]
        }
      } as ApplicationExceptions)
    );

    fixture.detectChanges();

    expect(fixture.componentInstance.rootException).not.toContain('Related Job');
  });

  it('falls back to "No Root Exception" when the history is empty', () => {
    loadExceptions.mockReturnValue(of({ exceptionHistory: { entries: [] } } as ApplicationExceptions));

    fixture.detectChanges();

    expect(fixture.componentInstance.rootException).toBe('No Root Exception');
  });
});
