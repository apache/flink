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

import { TaskManagerDetail, TaskManagerLogItem, TaskManagersItem } from '@flink-runtime-web/interfaces';
import { ProfilingDetail } from '@flink-runtime-web/interfaces/job-profiler';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { TaskManagerService } from './task-manager.service';

describe('TaskManagerService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn>; post: ReturnType<typeof vi.fn> };
  let configService: ConfigService;
  let service: TaskManagerService;

  beforeEach(() => {
    httpClient = { get: vi.fn(), post: vi.fn() };
    configService = new ConfigService();
    service = new TaskManagerService(httpClient as unknown as HttpClient, configService);
  });

  describe('loadManagers', () => {
    it('passes through the taskmanagers list on success', async () => {
      const manager = { id: 'tm-1' } as TaskManagersItem;
      httpClient.get.mockReturnValue(of({ taskmanagers: [manager] }));

      const result = await firstValueFrom(service.loadManagers());

      expect(result).toEqual([manager]);
    });

    it('falls back to an empty list when the response has no taskmanagers field', async () => {
      httpClient.get.mockReturnValue(of({}));

      const result = await firstValueFrom(service.loadManagers());

      expect(result).toEqual([]);
    });

    it('falls back to an empty list when the request fails', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('cluster unreachable')));

      const result = await firstValueFrom(service.loadManagers());

      expect(result).toEqual([]);
    });
  });

  describe('loadManager', () => {
    it('passes through the manager detail on success', async () => {
      const detail = { id: 'tm-1' } as TaskManagerDetail;
      httpClient.get.mockReturnValue(of(detail));

      const result = await firstValueFrom(service.loadManager('tm-1'));

      expect(result).toBe(detail);
    });

    it('swallows request errors into an empty stream, unlike loadManagers which falls back to []', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('cluster unreachable')));

      const emissions = await firstValueFrom(service.loadManager('tm-1').pipe(toArray()));

      expect(emissions).toEqual([]);
    });
  });

  describe('loadLogList', () => {
    it('extracts the logs array from the response', async () => {
      const log = { name: 'jobmanager.log', size: 100, mtime: 1 } as TaskManagerLogItem;
      httpClient.get.mockReturnValue(of({ logs: [log] }));

      const result = await firstValueFrom(service.loadLogList('tm-1'));

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/logs`);
      expect(result).toEqual([log]);
    });
  });

  describe('loadLog', () => {
    it('pairs the raw log text with the request url', async () => {
      httpClient.get.mockReturnValue(of('log contents'));

      const result = await firstValueFrom(service.loadLog('tm-1', 'jobmanager.log'));

      expect(result.data).toBe('log contents');
      expect(result.url).toBe(`${configService.BASE_URL}/taskmanagers/tm-1/logs/jobmanager.log`);
    });
  });

  describe('loadThreadDump', () => {
    it('joins the stringified thread infos into a single dump', async () => {
      httpClient.get.mockReturnValue(
        of({ threadInfos: [{ stringifiedThreadInfo: 'thread-A\n' }, { stringifiedThreadInfo: 'thread-B\n' }] })
      );

      const result = await firstValueFrom(service.loadThreadDump('tm-1'));

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/thread-dump`);
      expect(result).toBe('thread-A\nthread-B\n');
    });

    it('appends the mode query parameter when provided', async () => {
      httpClient.get.mockReturnValue(of({ threadInfos: [] }));

      await firstValueFrom(service.loadThreadDump('tm-1', 'full'));

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/thread-dump?mode=full`);
    });
  });

  describe('loadLogs', () => {
    it('returns the raw log text for the given TaskManager', async () => {
      httpClient.get.mockReturnValue(of('log contents'));

      const result = await firstValueFrom(service.loadLogs('tm-1'));

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/log`, expect.anything());
      expect(result).toBe('log contents');
    });
  });

  describe('loadStdout', () => {
    it('returns the raw stdout text for the given TaskManager', async () => {
      httpClient.get.mockReturnValue(of('stdout contents'));

      const result = await firstValueFrom(service.loadStdout('tm-1'));

      expect(httpClient.get).toHaveBeenCalledWith(
        `${configService.BASE_URL}/taskmanagers/tm-1/stdout`,
        expect.anything()
      );
      expect(result).toBe('stdout contents');
    });
  });

  describe('loadMetrics', () => {
    it('parses the metric values into a numeric map keyed by id and joins the requested names', async () => {
      httpClient.get.mockReturnValue(
        of([
          { id: 'Status.JVM.CPU.Load', value: '0.42' },
          { id: 'Status.JVM.Memory.Heap.Used', value: '128' }
        ])
      );

      const result = await firstValueFrom(
        service.loadMetrics('tm-1', ['Status.JVM.CPU.Load', 'Status.JVM.Memory.Heap.Used'])
      );

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/metrics`, {
        params: { get: 'Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used' }
      });
      expect(result).toEqual({ 'Status.JVM.CPU.Load': 0.42, 'Status.JVM.Memory.Heap.Used': 128 });
    });
  });

  describe('loadHistoryServerTaskManagerLogUrl', () => {
    it('extracts the log url from the response', async () => {
      httpClient.get.mockReturnValue(of({ url: 'http://history/log' }));

      const result = await firstValueFrom(service.loadHistoryServerTaskManagerLogUrl('job-1', 'tm-1'));

      expect(result).toBe('http://history/log');
    });
  });

  describe('loadProfilingList', () => {
    it('passes through the profiling list on success', async () => {
      const profilingList = { profiling: [] };
      httpClient.get.mockReturnValue(of(profilingList));

      const result = await firstValueFrom(service.loadProfilingList('tm-1'));

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/profiler`);
      expect(result).toBe(profilingList);
    });
  });

  describe('createProfilingInstance', () => {
    it('posts the requested profiling mode and duration', async () => {
      const profilingDetail = { status: 'RUNNING' } as ProfilingDetail;
      httpClient.post.mockReturnValue(of(profilingDetail));

      const result = await firstValueFrom(service.createProfilingInstance('tm-1', 'ITIMER', 30));

      expect(httpClient.post).toHaveBeenCalledWith(`${configService.BASE_URL}/taskmanagers/tm-1/profiler`, {
        mode: 'ITIMER',
        duration: 30
      });
      expect(result).toBe(profilingDetail);
    });
  });

  describe('loadProfilingResult', () => {
    it('pairs the raw profiling text with the request url', async () => {
      httpClient.get.mockReturnValue(of('profile contents'));

      const result = await firstValueFrom(service.loadProfilingResult('tm-1', 'profile.jfr'));

      expect(result.data).toBe('profile contents');
      expect(result.url).toBe(`${configService.BASE_URL}/taskmanagers/tm-1/profiler/profile.jfr`);
    });
  });
});
