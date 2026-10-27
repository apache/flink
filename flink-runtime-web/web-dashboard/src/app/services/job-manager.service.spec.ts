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
import { firstValueFrom, of } from 'rxjs';

import { ClusterConfiguration, EnvironmentInfo, JobManagerLogItem } from '@flink-runtime-web/interfaces';
import { ProfilingDetail, ProfilingList } from '@flink-runtime-web/interfaces/job-profiler';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { JobManagerService } from './job-manager.service';

describe('JobManagerService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn>; post: ReturnType<typeof vi.fn> };
  let service: JobManagerService;

  beforeEach(() => {
    httpClient = { get: vi.fn(), post: vi.fn() };
    service = new JobManagerService(httpClient as unknown as HttpClient, new ConfigService());
  });

  describe('loadConfig', () => {
    it('passes the cluster configuration through', async () => {
      const config = [{ key: 'k', value: 'v' }] as ClusterConfiguration[];
      httpClient.get.mockReturnValue(of(config));

      const result = await firstValueFrom(service.loadConfig());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/config');
      expect(result).toBe(config);
    });
  });

  describe('loadEnvironment', () => {
    it('passes the environment info through', async () => {
      const env = { jvm: {} } as unknown as EnvironmentInfo;
      httpClient.get.mockReturnValue(of(env));

      const result = await firstValueFrom(service.loadEnvironment());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/environment');
      expect(result).toBe(env);
    });
  });

  describe('loadLogs / loadStdout', () => {
    it('requests the plain log text with a no-cache header', async () => {
      httpClient.get.mockReturnValue(of('log contents'));

      const result = await firstValueFrom(service.loadLogs());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/log', expect.anything());
      expect(result).toBe('log contents');
    });

    it('requests the plain stdout text', async () => {
      httpClient.get.mockReturnValue(of('stdout contents'));

      const result = await firstValueFrom(service.loadStdout());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/stdout', expect.anything());
      expect(result).toBe('stdout contents');
    });
  });

  describe('loadLogList', () => {
    it('extracts the logs array from the response', async () => {
      const log = { name: 'jobmanager.log' } as JobManagerLogItem;
      httpClient.get.mockReturnValue(of({ logs: [log] }));

      const result = await firstValueFrom(service.loadLogList());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/logs');
      expect(result).toEqual([log]);
    });
  });

  describe('loadLog', () => {
    it('pairs the raw log text with the request url', async () => {
      httpClient.get.mockReturnValue(of('log contents'));

      const result = await firstValueFrom(service.loadLog('jobmanager.log'));

      expect(result.data).toBe('log contents');
      expect(result.url).toBe('./jobmanager/logs/jobmanager.log');
    });
  });

  describe('loadThreadDump', () => {
    it('joins the stringified thread infos into a single dump', async () => {
      httpClient.get.mockReturnValue(
        of({ threadInfos: [{ stringifiedThreadInfo: 'thread-A\n' }, { stringifiedThreadInfo: 'thread-B\n' }] })
      );

      const result = await firstValueFrom(service.loadThreadDump());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/thread-dump');
      expect(result).toBe('thread-A\nthread-B\n');
    });

    it('appends the mode query parameter when provided', async () => {
      httpClient.get.mockReturnValue(of({ threadInfos: [] }));

      await firstValueFrom(service.loadThreadDump('full'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/thread-dump?mode=full');
    });
  });

  describe('loadMetricsName', () => {
    it('maps the metric descriptors down to their ids', async () => {
      httpClient.get.mockReturnValue(of([{ id: 'Status.JVM.CPU.Load' }, { id: 'Status.JVM.Memory.Heap.Used' }]));

      const result = await firstValueFrom(service.loadMetricsName());

      expect(result).toEqual(['Status.JVM.CPU.Load', 'Status.JVM.Memory.Heap.Used']);
    });
  });

  describe('loadMetrics', () => {
    it('parses the metric values into a numeric map keyed by id', async () => {
      httpClient.get.mockReturnValue(of([{ id: 'Status.JVM.CPU.Load', value: '0.42' }]));

      const result = await firstValueFrom(service.loadMetrics(['Status.JVM.CPU.Load']));

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/metrics', { params: { get: 'Status.JVM.CPU.Load' } });
      expect(result).toEqual({ 'Status.JVM.CPU.Load': 0.42 });
    });
  });

  describe('loadHistoryServerConfig / loadHistoryServerEnvironment / loadHistoryServerJobManagerLogUrl', () => {
    it('requests the history-server cluster configuration for a job', async () => {
      const config = [] as ClusterConfiguration[];
      httpClient.get.mockReturnValue(of(config));

      const result = await firstValueFrom(service.loadHistoryServerConfig('job-1'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/jobmanager/config');
      expect(result).toBe(config);
    });

    it('requests the history-server environment for a job', async () => {
      const env = {} as EnvironmentInfo;
      httpClient.get.mockReturnValue(of(env));

      const result = await firstValueFrom(service.loadHistoryServerEnvironment('job-1'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/jobmanager/environment');
      expect(result).toBe(env);
    });

    it('extracts the history-server log url for a job', async () => {
      httpClient.get.mockReturnValue(of({ url: 'http://history/log' }));

      const result = await firstValueFrom(service.loadHistoryServerJobManagerLogUrl('job-1'));

      expect(httpClient.get).toHaveBeenCalledWith('./jobs/job-1/jobmanager/log-url');
      expect(result).toBe('http://history/log');
    });
  });

  describe('profiling', () => {
    it('passes the profiling list through', async () => {
      const list = { profiling: [] } as unknown as ProfilingList;
      httpClient.get.mockReturnValue(of(list));

      const result = await firstValueFrom(service.loadProfilingList());

      expect(httpClient.get).toHaveBeenCalledWith('./jobmanager/profiler');
      expect(result).toBe(list);
    });

    it('posts the requested profiling mode and duration', async () => {
      const detail = { status: 'RUNNING' } as ProfilingDetail;
      httpClient.post.mockReturnValue(of(detail));

      const result = await firstValueFrom(service.createProfilingInstance('ITIMER', 30));

      expect(httpClient.post).toHaveBeenCalledWith('./jobmanager/profiler', { mode: 'ITIMER', duration: 30 });
      expect(result).toBe(detail);
    });

    it('pairs the raw profiling result text with the request url', async () => {
      httpClient.get.mockReturnValue(of('profile contents'));

      const result = await firstValueFrom(service.loadProfilingResult('profile.jfr'));

      expect(result.data).toBe('profile contents');
      expect(result.url).toBe('./jobmanager/profiler/profile.jfr');
    });
  });
});
