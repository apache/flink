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

import type { Chart } from '@antv/g2';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobChartComponent } from './job-chart.component';
import { JobChartService } from './job-chart.service';

// Constructed directly: ngAfterViewInit builds a real G2 chart against a canvas jsdom can't
// provide, so these specs drive refresh()/close() with the chart instance left as a fake.
const cdr = { detectChanges: vi.fn(), detach: vi.fn(), markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

describe('JobChartComponent', () => {
  let component: JobChartComponent;

  beforeEach(() => {
    component = new JobChartComponent(cdr, new JobChartService());
    component.title = 'cpu';
  });

  it('appends the metric value for its own title and forwards data to the chart', () => {
    const changeData = vi.fn();
    component.chartInstance = { changeData } as unknown as Chart;

    component.refresh({ timestamp: 1000, values: { cpu: 42, mem: 7 } });

    expect(component.latestValue).toBe(42);
    expect(component.data).toEqual([{ time: 1000, value: 42, type: 'cpu' }]);
    expect(changeData).toHaveBeenCalledWith(component.data);
  });

  it('keeps only the most recent 20 samples', () => {
    for (let i = 0; i < 25; i++) {
      component.refresh({ timestamp: i, values: { cpu: i } });
    }

    expect(component.data).toHaveLength(20);
    expect(component.data[0].time).toBe(5);
    expect(component.data[19].time).toBe(24);
  });

  it('emits its title when closed', () => {
    const closed = vi.fn();
    component.closed.subscribe(closed);

    component.close();

    expect(closed).toHaveBeenCalledWith('cpu');
  });

  it('reports the big layout only when sized big', () => {
    expect(component.isBig).toBe(false);
    component.size = 'big';
    expect(component.isBig).toBe(true);
  });
});
