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

import { NgIf } from '@angular/common';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { EMPTY, of } from 'rxjs';

import { NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { JobService, MetricsService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzNotificationService } from 'ng-zorro-antd/notification';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewComponent } from './job-overview.component';
import { JobLocalService } from '../job-local.service';

const activatedRoute = { parent: { parent: { snapshot: { params: { jid: 'job-1' } } } } };

describe('JobOverviewComponent', () => {
  let fixture: ComponentFixture<JobOverviewComponent>;
  let element: HTMLElement;
  const navigate = vi.fn().mockResolvedValue(true);
  const changeDesiredParallelism = vi.fn();
  const success = vi.fn();

  beforeEach(async () => {
    navigate.mockClear();
    changeDesiredParallelism.mockReset().mockReturnValue(of(undefined));
    success.mockClear();
    // Replace the graph/list/resize children with stubs: the Dagre graph relies on
    // SVG layout APIs jsdom does not implement, and it is exercised by its own spec.
    await TestBed.configureTestingModule({
      imports: [JobOverviewComponent],
      providers: [
        { provide: Router, useValue: { navigate } },
        { provide: ActivatedRoute, useValue: activatedRoute },
        { provide: MetricsService, useValue: {} },
        { provide: JobService, useValue: { changeDesiredParallelism } },
        {
          provide: JobLocalService,
          useValue: { jobDetailChanges: () => EMPTY, selectedVertexChanges: () => EMPTY }
        },
        { provide: NzNotificationService, useValue: { success } }
      ]
    })
      .overrideComponent(JobOverviewComponent, {
        set: { imports: [NgIf, NzAlertModule], schemas: [CUSTOM_ELEMENTS_SCHEMA] }
      })
      .compileComponents();
    fixture = TestBed.createComponent(JobOverviewComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  it('shows the "not running yet" hint while no plan has arrived', () => {
    fixture.detectChanges();

    expect(fixture.componentInstance.nodes).toEqual([]);
    expect(element.textContent).toContain('Job is not running yet.');
  });

  it('navigates to a vertex on node click', () => {
    fixture.detectChanges();

    fixture.componentInstance.onNodeClick({ id: 'vertex-1' } as NodesItemCorrect);

    expect(navigate).toHaveBeenCalledWith(['vertex-1'], { relativeTo: expect.anything() });
  });

  it('requests a rescale and surfaces a success notification', () => {
    fixture.detectChanges();

    fixture.componentInstance.onRescale(new Map([['vertex-1', 4]]));

    expect(changeDesiredParallelism).toHaveBeenCalled();
    expect(success).toHaveBeenCalled();
  });
});
