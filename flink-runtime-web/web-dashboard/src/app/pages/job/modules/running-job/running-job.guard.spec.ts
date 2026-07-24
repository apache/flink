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

import { ActivatedRouteSnapshot, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { firstValueFrom, Observable, of } from 'rxjs';

import { JobsItem } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RunningJobGuard } from './running-job.guard';

function jobItem(overrides: Partial<JobsItem>): JobsItem {
  return { jid: 'job-1', name: 'test-job', state: 'RUNNING', completed: false, ...overrides } as JobsItem;
}

describe('RunningJobGuard', () => {
  let loadJobs: ReturnType<typeof vi.fn>;
  let navigate: ReturnType<typeof vi.fn>;
  let guard: RunningJobGuard;

  beforeEach(() => {
    loadJobs = vi.fn();
    navigate = vi.fn().mockResolvedValue(true);
    guard = new RunningJobGuard({ loadJobs } as unknown as JobService, { navigate } as unknown as Router);
  });

  function route(jid?: string): ActivatedRouteSnapshot {
    return { params: { jid } } as unknown as ActivatedRouteSnapshot;
  }

  function activate(jid?: string): Observable<boolean | UrlTree> {
    return guard.canActivate(route(jid), {} as RouterStateSnapshot) as Observable<boolean | UrlTree>;
  }

  it('denies activation when the route has no job id', () => {
    const result = guard.canActivate(route(undefined), {} as RouterStateSnapshot);

    expect(result).toBe(false);
    expect(loadJobs).not.toHaveBeenCalled();
  });

  it('redirects to the running-jobs list when the job cannot be found', async () => {
    loadJobs.mockReturnValue(of([jobItem({ jid: 'other-job' })]));

    const result = await firstValueFrom(activate('job-1'));

    expect(result).toBe(false);
    expect(navigate).toHaveBeenCalledWith(['/', 'job', 'running']);
  });

  it('redirects to the completed-job page when the job has already finished', async () => {
    loadJobs.mockReturnValue(of([jobItem({ jid: 'job-1', completed: true })]));

    const result = await firstValueFrom(activate('job-1'));

    expect(result).toBe(false);
    expect(navigate).toHaveBeenCalledWith(['/', 'job', 'completed', 'job-1']);
  });

  it('allows activation when the job exists and is still running', async () => {
    loadJobs.mockReturnValue(of([jobItem({ jid: 'job-1', completed: false })]));

    const result = await firstValueFrom(activate('job-1'));

    expect(result).toBe(true);
    expect(navigate).not.toHaveBeenCalled();
  });
});
