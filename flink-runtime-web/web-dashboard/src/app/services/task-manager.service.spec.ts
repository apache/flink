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

import { HttpClient, HttpContext } from '@angular/common/http';
import { firstValueFrom, of, throwError } from 'rxjs';

import { TaskManagerDetail, TaskManagersItem } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { EXPECTED_NOT_FOUND } from './http-context';
import { TaskManagerService } from './task-manager.service';

describe('TaskManagerService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn> };
  let service: TaskManagerService;

  beforeEach(() => {
    httpClient = { get: vi.fn() };
    service = new TaskManagerService(httpClient as unknown as HttpClient, new ConfigService());
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

    it('propagates request errors so callers can react to a gone TaskManager, unlike loadManagers', async () => {
      const error = new Error('cluster unreachable');
      httpClient.get.mockReturnValue(throwError(() => error));

      await expect(firstValueFrom(service.loadManager('tm-1'))).rejects.toBe(error);
    });

    it('marks the request as expecting a 404 so the interceptor does not surface it', () => {
      httpClient.get.mockReturnValue(of({ id: 'tm-1' }));

      service.loadManager('tm-1');

      const [, options] = httpClient.get.mock.calls[0] as [string, { context: HttpContext }];
      expect(options.context.get(EXPECTED_NOT_FOUND)).toBe(true);
    });
  });
});
