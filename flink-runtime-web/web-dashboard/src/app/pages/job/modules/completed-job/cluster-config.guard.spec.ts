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

import { ActivatedRouteSnapshot, Router, RouterStateSnapshot } from '@angular/router';

import { Configuration } from '@flink-runtime-web/interfaces';
import { StatusService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ClusterConfigGuard } from './cluster-config.guard';

describe('ClusterConfigGuard', () => {
  let statusService: StatusService;
  let navigate: ReturnType<typeof vi.fn>;
  let guard: ClusterConfigGuard;

  beforeEach(() => {
    statusService = {
      configuration: { features: { 'web-history': true } } as Configuration
    } as unknown as StatusService;
    navigate = vi.fn().mockResolvedValue(true);
    guard = new ClusterConfigGuard(statusService, { navigate } as unknown as Router);
  });

  function route(jid: string): ActivatedRouteSnapshot {
    return { parent: { params: { jid } } } as unknown as ActivatedRouteSnapshot;
  }

  it('allows activation when the history server feature is enabled', () => {
    const result = guard.canActivate(route('job-1'), {} as RouterStateSnapshot);

    expect(result).toBe(true);
    expect(navigate).not.toHaveBeenCalled();
  });

  it('redirects to the job overview and denies activation when the history server feature is disabled', () => {
    statusService.configuration.features['web-history'] = false;

    const result = guard.canActivate(route('job-1'), {} as RouterStateSnapshot);

    expect(result).toBe(false);
    expect(navigate).toHaveBeenCalledWith(['/', 'job', 'completed', 'job-1', 'overview']);
  });
});
