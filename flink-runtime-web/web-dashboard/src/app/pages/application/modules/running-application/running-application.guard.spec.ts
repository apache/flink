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

import { ApplicationItem } from '@flink-runtime-web/interfaces';
import { ApplicationService } from '@flink-runtime-web/services';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RunningApplicationGuard } from './running-application.guard';

function applicationItem(overrides: Partial<ApplicationItem>): ApplicationItem {
  return { id: 'app-1', name: 'test-app', status: 'RUNNING', completed: false, ...overrides } as ApplicationItem;
}

describe('RunningApplicationGuard', () => {
  let loadApplications: ReturnType<typeof vi.fn>;
  let navigate: ReturnType<typeof vi.fn>;
  let guard: RunningApplicationGuard;

  beforeEach(() => {
    loadApplications = vi.fn();
    navigate = vi.fn().mockResolvedValue(true);
    guard = new RunningApplicationGuard(
      { loadApplications } as unknown as ApplicationService,
      { navigate } as unknown as Router
    );
  });

  function route(id?: string): ActivatedRouteSnapshot {
    return { params: { id } } as unknown as ActivatedRouteSnapshot;
  }

  function activate(id?: string): Observable<boolean | UrlTree> {
    return guard.canActivate(route(id), {} as RouterStateSnapshot) as Observable<boolean | UrlTree>;
  }

  it('denies activation when the route has no application id', () => {
    const result = guard.canActivate(route(undefined), {} as RouterStateSnapshot);

    expect(result).toBe(false);
    expect(loadApplications).not.toHaveBeenCalled();
  });

  it('redirects to the running-applications list when the application cannot be found', async () => {
    loadApplications.mockReturnValue(of([applicationItem({ id: 'other-app' })]));

    const result = await firstValueFrom(activate('app-1'));

    expect(result).toBe(false);
    expect(navigate).toHaveBeenCalledWith(['/', 'application', 'running']);
  });

  it('redirects to the completed-application page when the application has already finished', async () => {
    loadApplications.mockReturnValue(of([applicationItem({ id: 'app-1', completed: true })]));

    const result = await firstValueFrom(activate('app-1'));

    expect(result).toBe(false);
    expect(navigate).toHaveBeenCalledWith(['/', 'application', 'completed', 'app-1']);
  });

  it('allows activation when the application exists and is still running', async () => {
    loadApplications.mockReturnValue(of([applicationItem({ id: 'app-1', completed: false })]));

    const result = await firstValueFrom(activate('app-1'));

    expect(result).toBe(true);
    expect(navigate).not.toHaveBeenCalled();
  });
});
