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

import { HttpClient } from '@angular/common/http';
import { firstValueFrom, of, throwError, toArray } from 'rxjs';

import { Overview } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { OverviewService } from './overview.service';

describe('OverviewService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn> };
  let service: OverviewService;

  beforeEach(() => {
    httpClient = { get: vi.fn() };
    service = new OverviewService(httpClient as unknown as HttpClient, new ConfigService());
  });

  it('passes the cluster overview through on success', async () => {
    const overview = { 'slots-total': 12 } as Overview;
    httpClient.get.mockReturnValue(of(overview));

    const result = await firstValueFrom(service.loadOverview());

    expect(httpClient.get).toHaveBeenCalledWith('./overview');
    expect(result).toBe(overview);
  });

  it('swallows request errors into an empty stream', async () => {
    httpClient.get.mockReturnValue(throwError(() => new Error('cluster unreachable')));

    const emissions = await firstValueFrom(service.loadOverview().pipe(toArray()));

    expect(emissions).toEqual([]);
  });
});
