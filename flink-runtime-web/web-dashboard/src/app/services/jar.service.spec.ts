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

import { HttpClient, HttpParams, HttpRequest } from '@angular/common/http';
import { firstValueFrom, of, throwError } from 'rxjs';

import { JarList, Plan } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { JarService } from './jar.service';

describe('JarService', () => {
  let httpClient: {
    get: ReturnType<typeof vi.fn>;
    delete: ReturnType<typeof vi.fn>;
    post: ReturnType<typeof vi.fn>;
    request: ReturnType<typeof vi.fn>;
  };
  let configService: ConfigService;
  let service: JarService;

  beforeEach(() => {
    httpClient = { get: vi.fn(), delete: vi.fn(), post: vi.fn(), request: vi.fn() };
    configService = new ConfigService();
    service = new JarService(httpClient as unknown as HttpClient, configService);
  });

  describe('loadJarList', () => {
    it('passes through a successful response', async () => {
      const jarList: JarList = { address: 'http://cluster', files: [] };
      httpClient.get.mockReturnValue(of(jarList));

      const result = await firstValueFrom(service.loadJarList());

      expect(httpClient.get).toHaveBeenCalledWith(`${configService.BASE_URL}/jars`);
      expect(result).toBe(jarList);
    });

    it('falls back to an empty error result when the request fails', async () => {
      httpClient.get.mockReturnValue(throwError(() => new Error('cluster unreachable')));

      const result = await firstValueFrom(service.loadJarList());

      expect(result).toEqual({ address: '', error: true, files: [] });
    });
  });

  describe('uploadJar', () => {
    it('wraps the file in form data and issues a progress-reporting POST request', () => {
      httpClient.request.mockReturnValue(of());
      const file = new File(['contents'], 'my-job.jar');

      service.uploadJar(file);

      expect(httpClient.request).toHaveBeenCalledTimes(1);
      const req = httpClient.request.mock.calls[0][0] as HttpRequest<FormData>;
      expect(req.method).toBe('POST');
      expect(req.url).toBe(`${configService.BASE_URL}/jars/upload`);
      expect(req.reportProgress).toBe(true);
      const uploaded = (req.body as FormData).get('jarfile') as File;
      expect(uploaded).toBeInstanceOf(File);
      expect(uploaded.name).toBe(file.name);
      expect(uploaded.size).toBe(file.size);
    });
  });

  describe('deleteJar', () => {
    it('issues a DELETE request for the given jar id', () => {
      httpClient.delete.mockReturnValue(of(undefined));

      service.deleteJar('jar-1');

      expect(httpClient.delete).toHaveBeenCalledWith(`${configService.BASE_URL}/jars/jar-1`);
    });
  });

  describe('runJob', () => {
    it('includes every provided argument as both a body field and a matching query param', () => {
      httpClient.post.mockReturnValue(of({ jobid: 'job-1' }));

      service.runJob('jar-1', 'org.example.Main', '4', '--foo bar', '/tmp/savepoint-1', 'true');

      expect(httpClient.post).toHaveBeenCalledTimes(1);
      const [url, body, options] = httpClient.post.mock.calls[0] as [string, unknown, { params: HttpParams }];
      expect(url).toBe(`${configService.BASE_URL}/jars/jar-1/run`);
      expect(body).toEqual({
        entryClass: 'org.example.Main',
        parallelism: '4',
        programArgs: '--foo bar',
        savepointPath: '/tmp/savepoint-1',
        allowNonRestoredState: 'true'
      });
      expect(options.params.get('entry-class')).toBe('org.example.Main');
      expect(options.params.get('parallelism')).toBe('4');
      expect(options.params.get('program-args')).toBe('--foo bar');
      // Regression guard: the savepoint path must land under its own query key, not get
      // shadowed by another field (see apache/flink#28722).
      expect(options.params.get('savepointPath')).toBe('/tmp/savepoint-1');
      expect(options.params.get('allowNonRestoredState')).toBe('true');
    });

    it('omits query params for arguments that are not provided', () => {
      httpClient.post.mockReturnValue(of({ jobid: 'job-1' }));

      service.runJob('jar-1', '', '', '', '', '');

      const options = httpClient.post.mock.calls[0][2] as { params: HttpParams };
      expect(options.params.get('entry-class')).toBeNull();
      expect(options.params.get('parallelism')).toBeNull();
      expect(options.params.get('program-args')).toBeNull();
      expect(options.params.get('savepointPath')).toBeNull();
      expect(options.params.get('allowNonRestoredState')).toBeNull();
    });
  });

  describe('getPlan', () => {
    it('builds vertex links from each node input and strips the detail field', async () => {
      const plan: Plan = {
        plan: {
          jid: 'job-1',
          name: 'test-job',
          nodes: [
            {
              id: 'node-2',
              parallelism: 1,
              operator: 'Map',
              operator_strategy: 'forward',
              description: 'Map',
              optimizer_properties: {},
              inputs: [{ num: 0, id: 'node-1', ship_strategy: 'FORWARD', exchange: 'pipelined' }]
            },
            {
              id: 'node-1',
              parallelism: 1,
              operator: 'Source',
              operator_strategy: 'forward',
              description: 'Source',
              optimizer_properties: {}
            }
          ]
        }
      };
      httpClient.get.mockReturnValue(of(plan));

      const result = await firstValueFrom(service.getPlan('jar-1', '', '', ''));

      expect(result.nodes).toHaveLength(2);
      expect(result.nodes.every(node => node.detail === undefined)).toBe(true);
      expect(result.links).toEqual([
        {
          num: 0,
          id: 'node-1-node-2',
          ship_strategy: 'FORWARD',
          exchange: 'pipelined',
          source: 'node-1',
          target: 'node-2'
        }
      ]);
    });

    it('returns empty nodes and links when the plan has no nodes', async () => {
      const plan: Plan = { plan: { jid: 'job-1', name: 'test-job', nodes: [] } };
      httpClient.get.mockReturnValue(of(plan));

      const result = await firstValueFrom(service.getPlan('jar-1', 'org.example.Main', '2', 'args'));

      expect(result).toEqual({ nodes: [], links: [] });
      const options = httpClient.get.mock.calls[0][1] as { params: HttpParams };
      expect(options.params.get('entry-class')).toBe('org.example.Main');
      expect(options.params.get('parallelism')).toBe('2');
      expect(options.params.get('program-args')).toBe('args');
    });
  });
});
