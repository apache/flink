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

import { NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { JOB_OVERVIEW_MODULE_DEFAULT_CONFIG } from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { describe, expect, it, vi } from 'vitest';

import { JobOverviewDrawerDetailComponent } from './job-overview-drawer-detail.component';
import { JobLocalService } from '../../job-local.service';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;

function createComponent(node: NodesItemCorrect | null): JobOverviewDrawerDetailComponent {
  const jobLocalService = { selectedVertexChanges: () => of(node) } as unknown as JobLocalService;
  return new JobOverviewDrawerDetailComponent(jobLocalService, cdr, JOB_OVERVIEW_MODULE_DEFAULT_CONFIG);
}

describe('JobOverviewDrawerDetailComponent', () => {
  it('rewrites <br/> tags in the vertex description to newlines', () => {
    const component = createComponent({ id: 'v1', description: 'line-a<br/>line-b<br/>line-c' } as NodesItemCorrect);

    component.ngOnInit();

    expect(component.node?.description).toBe('line-a\nline-b\nline-c');
  });

  it('leaves a description without <br/> untouched', () => {
    const component = createComponent({ id: 'v1', description: 'plain description' } as NodesItemCorrect);

    component.ngOnInit();

    expect(component.node?.description).toBe('plain description');
  });

  it('handles a null selected vertex without throwing', () => {
    const component = createComponent(null);

    expect(() => component.ngOnInit()).not.toThrow();
    expect(component.node).toBeNull();
  });
});
