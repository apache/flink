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

import { firstValueFrom } from 'rxjs';

import { JobDetailCorrect, NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it } from 'vitest';

import { JobLocalService } from './job-local.service';

describe('JobLocalService', () => {
  let service: JobLocalService;
  const job = { plan: { jid: 'job-1' } } as JobDetailCorrect;
  const vertex = { id: 'v1' } as NodesItemCorrect;

  beforeEach(() => {
    service = new JobLocalService();
  });

  it('replays the latest job detail', async () => {
    service.setJobDetail(job);

    expect(await firstValueFrom(service.jobDetailChanges())).toBe(job);
  });

  it('replays the latest selected vertex', async () => {
    service.setSelectedVertex(vertex);

    expect(await firstValueFrom(service.selectedVertexChanges())).toBe(vertex);
  });

  it('pairs the selected vertex with the latest job detail', async () => {
    service.setJobDetail(job);
    service.setSelectedVertex(vertex);

    expect(await firstValueFrom(service.jobWithVertexChanges())).toEqual({ job, vertex });
  });

  it('suppresses the paired stream while no vertex is selected', async () => {
    service.setJobDetail(job);
    // A null selection must be filtered out; the next real vertex is what surfaces.
    service.setSelectedVertex(null);
    service.setSelectedVertex(vertex);

    expect(await firstValueFrom(service.jobWithVertexChanges())).toEqual({ job, vertex });
  });

  it('exposes a metrics cache map', () => {
    expect(service.metricsCacheMap).toBeInstanceOf(Map);
  });
});
