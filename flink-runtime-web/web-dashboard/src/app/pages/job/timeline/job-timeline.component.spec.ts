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

import type { Chart } from '@antv/g2';
import { JobService, ConfigService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobTimelineComponent } from './job-timeline.component';
import { JobLocalService } from '../job-local.service';

const mockJobDetail = {
  jid: 'job-1',
  vertices: [{ id: 'v1', name: 'Source: generator', 'start-time': 1000, 'end-time': 2000, duration: 1000 }]
};

const mockSubTaskTimes = {
  id: 'v1',
  name: 'Source: generator',
  now: 3000,
  subtasks: [
    {
      subtask: 0,
      host: 'host-a',
      duration: 500,
      timestamps: {
        CREATED: 1000,
        RUNNING: 1200,
        FAILING: 0,
        RECONCILING: 0,
        CANCELLING: 0,
        RESTARTING: 0,
        FINISHED: 1700
      }
    }
  ]
};

// G2 charts draw to a canvas that jsdom cannot render, so the chart setup is
// replaced with lightweight fakes that record the data/render calls.
const createFakeChart = (): Chart =>
  ({ width: 800, changeSize: vi.fn(), data: vi.fn(), scale: vi.fn(), render: vi.fn() } as unknown as Chart);

describe('JobTimelineComponent', () => {
  let fixture: ComponentFixture<JobTimelineComponent>;
  let component: JobTimelineComponent;
  let fakeMain: Chart;
  let fakeSub: Chart;
  const loadSubTaskTimes = vi.fn();

  beforeEach(async () => {
    loadSubTaskTimes.mockReset().mockReturnValue(of(mockSubTaskTimes));
    await TestBed.configureTestingModule({
      imports: [JobTimelineComponent],
      providers: [
        { provide: ConfigService, useValue: { COLOR_MAP: {} } },
        { provide: JobService, useValue: { loadSubTaskTimes } },
        { provide: JobLocalService, useValue: { jobDetailChanges: () => of(mockJobDetail) } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobTimelineComponent);
    component = fixture.componentInstance;

    fakeMain = createFakeChart();
    fakeSub = createFakeChart();
    vi.spyOn(component, 'setUpMainChart').mockImplementation(() => {
      component.mainChartInstance = fakeMain;
    });
    vi.spyOn(component, 'setUpSubTaskChart').mockImplementation(() => {
      component.subTaskChartInstance = fakeSub;
    });
  });

  it('maps the vertices into timeline ranges and renders the main chart', () => {
    fixture.detectChanges();

    expect(component.jobDetail).toEqual(mockJobDetail);
    expect(component.listOfVertex).toHaveLength(1);
    expect(component.listOfVertex[0].range).toEqual([1000, 2000]);
    expect(fakeMain.data).toHaveBeenCalledWith(component.listOfVertex);
    expect(fakeMain.render).toHaveBeenCalled();
  });

  it('builds the subtask timeline from subtask time stamps', () => {
    fixture.detectChanges();

    component.updateSubTaskChart('v1');

    expect(loadSubTaskTimes).toHaveBeenCalledWith('job-1', 'v1');
    expect(component.isShowSubTaskTimeLine).toBe(true);
    expect(component.listOfSubTaskTimeLine.length).toBeGreaterThan(0);
    expect(component.listOfSubTaskTimeLine.every(item => item.name === '0 - host-a')).toBe(true);
    expect(fakeSub.render).toHaveBeenCalled();
  });
});
