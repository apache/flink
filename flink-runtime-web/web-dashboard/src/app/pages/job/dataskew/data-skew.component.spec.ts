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
import { MetricsService } from '@flink-runtime-web/services';
import { provideNzIcons } from 'ng-zorro-antd/icon';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DataSkewComponent } from './data-skew.component';
import { JobLocalService } from '../job-local.service';

const mockJobDetail = {
  jid: 'job-1',
  vertices: [
    { id: 'v1', name: 'Source: generator' },
    { id: 'v2', name: 'Sink: out' }
  ]
};

describe('DataSkewComponent', () => {
  let fixture: ComponentFixture<DataSkewComponent>;
  let element: HTMLElement;
  const loadAggregatedMetrics = vi.fn();

  beforeEach(async () => {
    // Return a higher skew for the first vertex so ordering can be asserted.
    loadAggregatedMetrics
      .mockReset()
      .mockImplementation((_jid: string, vertexId: string) => of({ numRecordsIn: vertexId === 'v1' ? 42 : 7 }));
    await TestBed.configureTestingModule({
      imports: [DataSkewComponent],
      providers: [
        provideNzIcons(APP_ICONS),
        { provide: MetricsService, useValue: { loadAggregatedMetrics } },
        { provide: JobLocalService, useValue: { jobDetailChanges: () => of(mockJobDetail) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(DataSkewComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('loads per-vertex skew and renders it sorted by descending percentage', () => {
    fixture.detectChanges();

    expect(loadAggregatedMetrics).toHaveBeenCalledWith('job-1', 'v1', ['numRecordsIn'], 'skew');
    expect(fixture.componentInstance.isLoading).toBe(false);
    expect(fixture.componentInstance.listOfVerticesAndSkew).toEqual([
      { vertexName: 'Source: generator', skewPct: 42 },
      { vertexName: 'Sink: out', skewPct: 7 }
    ]);

    const text = element.textContent ?? '';
    expect(text).toContain('What is Data Skew?');
    expect(text).toContain('Source: generator');
    expect(text).toContain('42%');
  });

  it('treats a non-numeric skew metric as zero', () => {
    loadAggregatedMetrics.mockReturnValue(of({ numRecordsIn: NaN }));

    fixture.detectChanges();

    expect(fixture.componentInstance.listOfVerticesAndSkew.every(v => v.skewPct === 0)).toBe(true);
  });
});
