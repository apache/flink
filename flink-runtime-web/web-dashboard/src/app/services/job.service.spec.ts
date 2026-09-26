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

import { JobDetail, JobOverview } from '@flink-runtime-web/interfaces';
import { JobResourceRequirements } from '@flink-runtime-web/interfaces/job-resource-requirements';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { JobService } from './job.service';

describe('JobService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn>; put: ReturnType<typeof vi.fn> };
  let service: JobService;

  beforeEach(() => {
    httpClient = { get: vi.fn(), put: vi.fn() };
    service = new JobService(httpClient as unknown as HttpClient, new ConfigService());
  });

  describe('loadJobs', () => {
    it('uppercases task-state keys, folds in pending operators, and derives the completed flag', async () => {
      const overview = {
        jobs: [
          { jid: 'job-1', state: 'RUNNING', tasks: { running: 2, finished: 1 }, 'pending-operators': 4 },
          { jid: 'job-2', state: 'FINISHED', tasks: {} }
        ]
      } as unknown as JobOverview;
      httpClient.get.mockReturnValue(of(overview));

      const result = await firstValueFrom(service.loadJobs());

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/overview');
      expect(result[0].tasks).toEqual({ RUNNING: 2, FINISHED: 1, PENDING: 4 });
      expect(result[0].completed).toBe(false);
      // No pending-operators field falls back to 0, and FINISHED counts as completed.
      expect(result[1].tasks).toEqual({ PENDING: 0 });
      expect(result[1].completed).toBe(true);
    });

    it('swallows request errors into an empty stream', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('boom')));

      const emissions = await firstValueFrom(service.loadJobs().pipe(toArray()));

      expect(emissions).toEqual([]);
    });
  });

  describe('loadJob', () => {
    it('builds graph nodes/links from the plan and stream graph and sorts nodes by vertex order', async () => {
      const job = {
        jid: 'job-1',
        vertices: [{ id: 'v1' }, { id: 'v2' }],
        plan: {
          jid: 'job-1',
          // Deliberately out of vertex order so the sort has something to do.
          nodes: [{ id: 'v2', inputs: [{ id: 'v1' }] }, { id: 'v1' }]
        },
        'stream-graph': { nodes: [{ id: 'sn1', inputs: [{ id: 'sn0' }] }, { id: 'sn0' }] },
        'status-counts': {},
        'pending-operators': 3
      } as unknown as JobDetail;
      httpClient.get.mockReturnValue(of(job));

      const result = await firstValueFrom(service.loadJob('job-1'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1');
      expect(result.plan.nodes.map(n => n.id)).toEqual(['v1', 'v2']);
      expect(result.plan.nodes[0]).toMatchObject({ id: 'v1', job_vertex_id: 'v1', detail: { id: 'v1' } });
      expect(result.plan.links).toEqual([{ id: 'v1-v2', source: 'v1', target: 'v2' }]);
      expect(result.plan.streamNodes.map(n => n.id)).toEqual(['sn1', 'sn0']);
      expect(result.plan.streamLinks).toEqual([{ id: 'sn0-sn1', source: 'sn0', target: 'sn1' }]);
      // The stream graph carries pending operators into the status counts.
      expect(result['status-counts']['PENDING']).toBe(3);
    });

    it('swallows request errors into an empty stream', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('boom')));

      const emissions = await firstValueFrom(service.loadJob('job-1').pipe(toArray()));

      expect(emissions).toEqual([]);
    });
  });

  describe('loadAccumulators', () => {
    it('combines the user and subtask accumulator responses', async () => {
      httpClient.get
        .mockReturnValueOnce(of({ 'user-accumulators': [{ name: 'records' }] }))
        .mockReturnValueOnce(of({ subtasks: [{ subtask: 0 }] }));

      const result = await firstValueFrom(service.loadAccumulators('job-1', 'v1'));

      expect(httpClient.get).toHaveBeenNthCalledWith(1, './jobs/job-1/vertices/v1/accumulators');
      expect(httpClient.get).toHaveBeenNthCalledWith(2, './jobs/job-1/vertices/v1/subtasks/accumulators');
      expect(result).toEqual({ main: [{ name: 'records' }], subtasks: [{ subtask: 0 }] });
    });
  });

  describe('changeDesiredParallelism', () => {
    it('raises the upper bound only for the requested vertices and puts the updated requirements', async () => {
      const requirements: JobResourceRequirements = {
        v1: { parallelism: { lowerBound: 1, upperBound: 2 } },
        v2: { parallelism: { lowerBound: 1, upperBound: 2 } }
      };
      httpClient.get.mockReturnValue(of(requirements));
      httpClient.put.mockReturnValue(of(undefined));

      await firstValueFrom(service.changeDesiredParallelism('job-1', new Map([['v1', 4]])));

      expect(httpClient.put).toHaveBeenCalledWith('./jobs/job-1/resource-requirements', {
        v1: { parallelism: { lowerBound: 1, upperBound: 4 } },
        v2: { parallelism: { lowerBound: 1, upperBound: 2 } }
      });
    });
  });

  describe('URL construction', () => {
    it('cancels a job through the yarn-cancel handler', async () => {
      httpClient.get.mockReturnValue(of(undefined));

      await firstValueFrom(service.cancelJob('job-1'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/yarn-cancel');
    });

    it('passes the max-exceptions cap as a query parameter', async () => {
      httpClient.get.mockReturnValue(of({}));

      await firstValueFrom(service.loadExceptions('job-1', 25));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/exceptions?maxExceptions=25');
    });

    it('embeds the checkpoint id in the details path', async () => {
      httpClient.get.mockReturnValue(of({}));

      await firstValueFrom(service.loadCheckpointDetails('job-1', 7));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/checkpoints/details/7');
    });

    it('passes both the flame-graph type and subtask index', async () => {
      httpClient.get.mockReturnValue(of({}));

      await firstValueFrom(service.loadOperatorFlameGraphForSingleSubtask('job-1', 'v1', 'ON_CPU', '3'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/vertices/v1/flamegraph?type=ON_CPU&subtaskindex=3');
    });
  });
});
