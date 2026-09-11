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
import { of } from 'rxjs';

import { MetricsService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerChartComponent } from './job-overview-drawer-chart.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobOverviewDrawerChartComponent', () => {
  const loadAllAvailableMetrics = vi.fn();
  let metricsCacheMap: Map<string, string[]>;
  let component: JobOverviewDrawerChartComponent;

  beforeEach(() => {
    loadAllAvailableMetrics.mockReset().mockReturnValue(of([{ id: 'm1' }, { id: 'm2' }, { id: 'm3' }]));
    metricsCacheMap = new Map();
    const jobLocalService = {
      jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } }),
      metricsCacheMap
    } as unknown as JobLocalService;
    component = new JobOverviewDrawerChartComponent(
      { loadAllAvailableMetrics, loadMetrics: vi.fn().mockReturnValue(of({})) } as unknown as MetricsService,
      jobLocalService,
      cdr
    );
  });

  it('lists all available metrics as unselected when nothing is cached', () => {
    component.ngOnInit();

    expect(component.cacheMetricKey).toBe('job-1/v1');
    expect(component.listOfMetricName).toEqual(['m1', 'm2', 'm3']);
    expect(component.listOfSelectedMetric).toEqual([]);
    expect(component.listOfUnselectedMetric).toEqual(['m1', 'm2', 'm3']);
  });

  it('restores the previously selected metrics from the per-vertex cache', () => {
    metricsCacheMap.set('job-1/v1', ['m2']);

    component.ngOnInit();

    expect(component.listOfSelectedMetric).toEqual(['m2']);
    expect(component.listOfUnselectedMetric).toEqual(['m1', 'm3']);
  });

  it('moves a metric between the selected and unselected lists and writes through to the cache', () => {
    component.ngOnInit();

    component.updateMetric('m1');
    expect(component.listOfSelectedMetric).toEqual(['m1']);
    expect(component.listOfUnselectedMetric).toEqual(['m2', 'm3']);
    expect(metricsCacheMap.get('job-1/v1')).toEqual(['m1']);

    component.closeMetric('m1');
    expect(component.listOfSelectedMetric).toEqual([]);
    expect(component.listOfUnselectedMetric).toEqual(['m1', 'm2', 'm3']);
    expect(metricsCacheMap.get('job-1/v1')).toEqual([]);
  });
});
