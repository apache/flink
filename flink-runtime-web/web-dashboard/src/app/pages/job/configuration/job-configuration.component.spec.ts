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

import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobConfigurationComponent } from './job-configuration.component';
import { JobLocalService } from '../job-local.service';

const mockConfig = {
  jid: 'job-1',
  name: 'demo',
  'execution-config': {
    'execution-mode': 'PIPELINED',
    'restart-strategy': 'Cluster level default restart strategy',
    'job-parallelism': 4,
    'object-reuse-mode': false,
    'user-config': {
      'state.backend': 'hashmap',
      'pipeline.name': 'my-pipeline'
    }
  }
};

describe('JobConfigurationComponent', () => {
  let fixture: ComponentFixture<JobConfigurationComponent>;
  let element: HTMLElement;
  const loadJobConfig = vi.fn();

  beforeEach(async () => {
    loadJobConfig.mockReset().mockReturnValue(of(mockConfig));
    await TestBed.configureTestingModule({
      imports: [JobConfigurationComponent],
      providers: [
        { provide: JobService, useValue: { loadJobConfig } },
        { provide: JobLocalService, useValue: { jobDetailChanges: () => of({ jid: 'job-1' }) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobConfigurationComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads the job config and lists user config entries sorted by key', () => {
    fixture.detectChanges();

    expect(loadJobConfig).toHaveBeenCalledWith('job-1');
    expect(fixture.componentInstance.config).toEqual(mockConfig);
    expect(fixture.componentInstance.listOfUserConfig).toEqual([
      { key: 'pipeline.name', value: 'my-pipeline' },
      { key: 'state.backend', value: 'hashmap' }
    ]);

    const text = element.textContent ?? '';
    expect(text).toContain('Execution Configuration');
    expect(text).toContain('User Configuration');
    expect(text).toContain('pipeline.name');
    expect(text).toContain('my-pipeline');
  });
});
