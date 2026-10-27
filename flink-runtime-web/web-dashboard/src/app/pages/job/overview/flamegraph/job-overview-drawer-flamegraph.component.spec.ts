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
import { of, throwError } from 'rxjs';

import { FlameGraphType } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerFlameGraphComponent } from './job-overview-drawer-flamegraph.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const jobLocalService = {
  jobWithVertexChanges: () => of({ job: { jid: 'job-1' }, vertex: { id: 'v1' } })
} as unknown as JobLocalService;

describe('JobOverviewDrawerFlameGraphComponent', () => {
  const loadOperatorFlameGraph = vi.fn();
  const loadOperatorFlameGraphForSingleSubtask = vi.fn();
  const loadSubTasks = vi.fn();
  let component: JobOverviewDrawerFlameGraphComponent;

  beforeEach(() => {
    loadOperatorFlameGraph.mockReset().mockReturnValue(of({ endTimestamp: 123 }));
    loadOperatorFlameGraphForSingleSubtask.mockReset().mockReturnValue(of({ endTimestamp: 456 }));
    loadSubTasks.mockReset().mockReturnValue(of({ subtasks: [] }));
    component = new JobOverviewDrawerFlameGraphComponent(
      { loadOperatorFlameGraph, loadOperatorFlameGraphForSingleSubtask, loadSubTasks } as unknown as JobService,
      jobLocalService,
      cdr
    );
  });

  it('offers only running/initializing subtasks (plus "all") as sampleable', () => {
    loadSubTasks.mockReturnValue(
      of({
        subtasks: [
          { subtask: 0, status: 'RUNNING' },
          { subtask: 1, status: 'FINISHED' },
          { subtask: 2, status: 'INITIALIZING' }
        ]
      })
    );

    component.ngOnInit();

    expect(component.listOfSampleableSubtasks).toEqual(['all', '0', '2']);
  });

  it('loads the whole-vertex flame graph by default and stores the graph type', () => {
    component.ngOnInit();

    expect(loadOperatorFlameGraph).toHaveBeenCalledWith('job-1', 'v1', FlameGraphType.ON_CPU);
    expect(component.flameGraph.endTimestamp).toBe(123);
    expect(component.flameGraph.graphType).toBe(FlameGraphType.ON_CPU);
    expect(component.isLoading).toBe(false);
  });

  it('switches to the single-subtask flame graph once a subtask is selected', () => {
    component.ngOnInit();

    component.selectSubtask('0');

    expect(loadOperatorFlameGraphForSingleSubtask).toHaveBeenCalledWith('job-1', 'v1', FlameGraphType.ON_CPU, '0');
    expect(component.flameGraph.endTimestamp).toBe(456);
  });

  it('keeps just "all" sampleable when the subtasks request fails', () => {
    loadSubTasks.mockReturnValue(throwError(() => new Error('boom')));

    component.ngOnInit();

    expect(component.listOfSampleableSubtasks).toEqual(['all']);
  });
});
