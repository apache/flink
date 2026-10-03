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
  vertices: [
    // No end-time recorded yet: the range end must be derived from start-time + duration.
    { id: 'v2', name: 'Sink: writer', 'start-time': 3000, 'end-time': -1, duration: 500 },
    { id: 'v1', name: 'Source: generator', 'start-time': 1000, 'end-time': 2000, duration: 1000 },
    // Not started yet: must be filtered out of the timeline entirely.
    { id: 'v3', name: 'Not started yet', 'start-time': -1, 'end-time': -1, duration: 0 }
  ]
};

const mockSubTaskTimes = {
  id: 'v1',
  name: 'Source: generator',
  now: 3000,
  subtasks: [
    {
      subtask: 0,
      endpoint: 'host-a',
      duration: 1000,
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
  ({ width: 800, changeSize: vi.fn(), data: vi.fn(), scale: vi.fn(), render: vi.fn() }) as unknown as Chart;

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

  it('maps the vertices into timeline ranges, filtering unstarted ones and sorting by start time', () => {
    fixture.detectChanges();

    expect(component.jobDetail).toEqual(mockJobDetail);
    expect(component.listOfVertex.map(v => v.id)).toEqual(['v1', 'v2']);
    expect(component.listOfVertex[0].range).toEqual([1000, 2000]);
    expect(component.listOfVertex[1].range).toEqual([3000, 3500]);
    expect(fakeMain.data).toHaveBeenCalledWith(component.listOfVertex);
    expect(fakeMain.render).toHaveBeenCalled();
  });

  it('derives each subtask timeline range from its ordered status timestamps', () => {
    fixture.detectChanges();

    component.updateSubTaskChart('v1');

    expect(loadSubTaskTimes).toHaveBeenCalledWith('job-1', 'v1');
    expect(component.isShowSubTaskTimeLine).toBe(true);
    expect(component.listOfSubTaskTimeLine).toEqual([
      { name: '0 - host-a', status: 'CREATED', range: [1000, 1200] },
      { name: '0 - host-a', status: 'RUNNING', range: [1200, 1700] },
      // The final status runs until the subtask's overall finish (first start + duration).
      { name: '0 - host-a', status: 'FINISHED', range: [1700, 2000] }
    ]);
    expect(fakeSub.data).toHaveBeenCalledWith(component.listOfSubTaskTimeLine);
    expect(fakeSub.render).toHaveBeenCalled();
  });
});
