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

import { APP_ICONS } from '@flink-runtime-web/app-icons';
import { NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { StatusService } from '@flink-runtime-web/services';
import { provideNzIcons } from 'ng-zorro-antd/icon';
import { describe, expect, it } from 'vitest';

import { JobOverviewListComponent } from './job-overview-list.component';

const node = {
  id: 'vertex-1',
  parallelism: 2,
  detail: { name: 'Source', status: 'RUNNING', 'start-time': 0, 'end-time': -1, duration: 0 }
} as unknown as NodesItemCorrect;

async function render(
  webRescale: boolean,
  rescaleSupported: boolean
): Promise<ComponentFixture<JobOverviewListComponent>> {
  await TestBed.configureTestingModule({
    imports: [JobOverviewListComponent],
    providers: [
      provideNzIcons(APP_ICONS),
      { provide: StatusService, useValue: { configuration: { features: { 'web-rescale': webRescale } } } }
    ]
  }).compileComponents();
  const fixture = TestBed.createComponent(JobOverviewListComponent);
  fixture.componentRef.setInput('nodes', [node]);
  fixture.componentRef.setInput('rescaleSupported', rescaleSupported);
  fixture.detectChanges();
  return fixture;
}

function headers(fixture: ComponentFixture<JobOverviewListComponent>): string[] {
  const element = fixture.nativeElement as HTMLElement;
  return Array.from(element.querySelectorAll('thead th')).map(th => th.textContent!.trim());
}

describe('JobOverviewListComponent rescale controls', () => {
  it('shows the Scale column when the cluster allows it and the job supports rescaling', async () => {
    const fixture = await render(true, true);

    expect(headers(fixture)).toContain('Scale');
  });

  it('hides the Scale column when the job does not support rescaling', async () => {
    const fixture = await render(true, false);

    expect(headers(fixture)).not.toContain('Scale');
  });

  it('hides the Scale column when web rescaling is disabled for the cluster', async () => {
    const fixture = await render(false, true);

    expect(headers(fixture)).not.toContain('Scale');
  });
});
