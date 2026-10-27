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
import { ChangeDetectorRef, CUSTOM_ELEMENTS_SCHEMA, ElementRef } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { EMPTY, of } from 'rxjs';

import { JobDetailCorrect, NodesItemCorrect, NodesItemLink } from '@flink-runtime-web/interfaces';
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
    // SVG layout APIs jsdom does not implement.
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

  it('requests a rescale for the current job and surfaces a success notification', () => {
    fixture.detectChanges();
    const component = fixture.componentInstance;
    component.jobId = 'job-1';
    const desiredParallelism = new Map([['vertex-1', 4]]);

    component.onRescale(desiredParallelism);

    expect(changeDesiredParallelism).toHaveBeenCalledWith('job-1', desiredParallelism);
    expect(success).toHaveBeenCalledWith(
      'Rescaling operation.',
      'Job resources requirements have been updated. Job will now try to rescale.'
    );
  });
});

describe('JobOverviewComponent with a resolved plan', () => {
  // The Dagre graph relies on SVG layout APIs jsdom does not implement, so the component is
  // constructed directly (bypassing TestBed/change detection) and given a fake dagreComponent,
  // rather than trying to render the real child through the view.
  const mockPlan: JobDetailCorrect['plan'] = {
    jid: 'job-1',
    name: 'Test Job',
    type: 'STREAMING',
    nodes: [{ id: 'vertex-a' } as NodesItemCorrect],
    links: [] as NodesItemLink[],
    streamNodes: [
      { id: 'node-a', job_vertex_id: 'vertex-a' } as NodesItemCorrect,
      { id: 'node-b' } as NodesItemCorrect
    ],
    streamLinks: [{ id: 'link-1', source: 'node-a', target: 'node-b' } as NodesItemLink]
  };

  function createComponent(): {
    component: JobOverviewComponent;
    fakeDagre: { showPendingOperators: boolean; flush: ReturnType<typeof vi.fn>; updateNode: ReturnType<typeof vi.fn> };
  } {
    const fakeDagre = {
      showPendingOperators: false,
      flush: vi.fn().mockResolvedValue(undefined),
      updateNode: vi.fn()
    };
    const component = new JobOverviewComponent(
      {} as unknown as Router,
      activatedRoute as unknown as ActivatedRoute,
      {} as ElementRef,
      {
        loadMetricsWithAllAggregates: vi.fn().mockReturnValue(of({})),
        loadWatermarks: vi.fn().mockReturnValue(of({ lowWatermark: NaN }))
      } as unknown as MetricsService,
      {
        jobDetailChanges: () => of({ jid: 'job-1', plan: mockPlan } as JobDetailCorrect),
        selectedVertexChanges: () => EMPTY
      } as unknown as JobLocalService,
      {} as unknown as JobService,
      {} as unknown as NzNotificationService,
      { markForCheck: vi.fn() } as unknown as ChangeDetectorRef
    );
    (component as unknown as { dagreComponent: typeof fakeDagre }).dagreComponent = fakeDagre;
    return { component, fakeDagre };
  }

  it('derives pending nodes and links from the streaming graph when a plan arrives', () => {
    const { component } = createComponent();
    component.ngOnInit();

    expect(component.nodes).toEqual(mockPlan.nodes);
    expect(component.pendingNodes).toEqual([{ id: 'node-b' }]);
    // The pending link's endpoints are remapped through the streaming-graph node ids onto
    // their job-vertex ids, so 'node-a' becomes 'vertex-a' while the still-pending 'node-b'
    // (no job vertex yet) is left as-is.
    expect(component.pendingLinks).toEqual([
      { id: 'vertex-a-node-b', source: 'vertex-a', target: 'node-b', pending: true }
    ]);
  });

  it('flushes the dagre graph with the resolved nodes and links', () => {
    const { component, fakeDagre } = createComponent();
    component.ngOnInit();

    expect(fakeDagre.flush).toHaveBeenCalledWith(mockPlan.nodes, mockPlan.links, true);
  });

  it('flushes the pending nodes and links alongside the resolved ones when pending operators are shown', () => {
    const { component, fakeDagre } = createComponent();
    fakeDagre.showPendingOperators = true;
    component.ngOnInit();

    expect(fakeDagre.flush).toHaveBeenCalledWith(
      [...mockPlan.nodes, { id: 'node-b' }],
      [{ id: 'vertex-a-node-b', source: 'vertex-a', target: 'node-b', pending: true }],
      true
    );
  });
});
