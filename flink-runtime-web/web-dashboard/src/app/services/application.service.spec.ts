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

import { ApplicationDetail, ApplicationOverview } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApplicationService } from './application.service';
import { ConfigService } from './config.service';

describe('ApplicationService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn>; post: ReturnType<typeof vi.fn> };
  let service: ApplicationService;

  beforeEach(() => {
    httpClient = { get: vi.fn(), post: vi.fn() };
    service = new ApplicationService(httpClient as unknown as HttpClient, new ConfigService());
  });

  describe('loadApplications', () => {
    it('sums each job-status count into a TOTAL and derives the completed flag from status', async () => {
      const overview: ApplicationOverview = {
        applications: [
          {
            id: 'app-1',
            name: 'running-app',
            status: 'RUNNING',
            'start-time': 0,
            'end-time': -1,
            duration: 100,
            jobs: { RUNNING: 2, FINISHED: 1 } as ApplicationOverview['applications'][0]['jobs']
          },
          {
            id: 'app-2',
            name: 'finished-app',
            status: 'FINISHED',
            'start-time': 0,
            'end-time': 100,
            duration: 100,
            jobs: { FINISHED: 3 } as ApplicationOverview['applications'][0]['jobs']
          }
        ]
      };
      httpClient.get.mockReturnValue(of(overview));

      const result = await firstValueFrom(service.loadApplications());

      expect(result[0].jobs.TOTAL).toBe(3);
      expect(result[0].completed).toBe(false);
      expect(result[1].jobs.TOTAL).toBe(3);
      expect(result[1].completed).toBe(true);
    });

    it('swallows request errors into an empty stream', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('cluster unreachable')));

      const emissions = await firstValueFrom(service.loadApplications().pipe(toArray()));

      expect(emissions).toEqual([]);
    });
  });

  describe('loadApplication', () => {
    function detail(): ApplicationDetail {
      return {
        id: 'app-1',
        name: 'test-app',
        status: 'RUNNING',
        'start-time': 0,
        'end-time': -1,
        duration: 100,
        timestamps: {} as ApplicationDetail['timestamps'],
        jobs: [
          {
            jid: 'job-1',
            name: 'job-one',
            state: 'RUNNING',
            'start-time': 0,
            'end-time': -1,
            duration: 100,
            'last-modification': 0,
            'pending-operators': 2,
            // Raw REST payload uses lowercase task-state keys; the service normalizes them to uppercase.
            tasks: { running: 3, finished: 1 } as unknown as ApplicationDetail['jobs'][0]['tasks']
          },
          {
            jid: 'job-2',
            name: 'job-two',
            state: 'FINISHED',
            'start-time': 0,
            'end-time': 100,
            duration: 100,
            'last-modification': 100,
            tasks: { finished: 4 } as unknown as ApplicationDetail['jobs'][0]['tasks']
          }
        ]
      };
    }

    it('normalizes task-status keys to uppercase and backfills PENDING from pending-operators', async () => {
      httpClient.get.mockReturnValue(of(detail()));

      const result = await firstValueFrom(service.loadApplication('app-1'));

      expect(result.jobs[0].tasks).toEqual({ RUNNING: 3, FINISHED: 1, PENDING: 2 });
      expect(result.jobs[0].tasks).not.toHaveProperty('running');
      // No pending-operators on job-two, so PENDING backfills to 0.
      expect(result.jobs[1].tasks).toEqual({ FINISHED: 4, PENDING: 0 });
    });

    it('derives each job completed flag from its state', async () => {
      httpClient.get.mockReturnValue(of(detail()));

      const result = await firstValueFrom(service.loadApplication('app-1'));

      expect(result.jobs[0].completed).toBe(false);
      expect(result.jobs[1].completed).toBe(true);
    });

    it('tallies per-state job counts into status-counts', async () => {
      httpClient.get.mockReturnValue(of(detail()));

      const result = await firstValueFrom(service.loadApplication('app-1'));

      expect(result['status-counts']?.RUNNING).toBe(1);
      expect(result['status-counts']?.FINISHED).toBe(1);
      expect(result['status-counts']?.TOTAL).toBe(2);
    });

    it('swallows request errors into an empty stream', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('cluster unreachable')));

      const emissions = await firstValueFrom(service.loadApplication('app-1').pipe(toArray()));

      expect(emissions).toEqual([]);
    });
  });
});
