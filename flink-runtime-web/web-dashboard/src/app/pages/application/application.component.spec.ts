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

import { ChangeDetectorRef } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Subject } from 'rxjs';

import { ApplicationItem } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApplicationComponent } from './application.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const activatedRoute = {} as ActivatedRoute;
const seg = (path: string): { path: string; toString: () => string } => ({ path, toString: () => path });

function urlTree(paths: string[]): unknown {
  return { root: { children: { primary: { segments: paths.map(seg) } } } };
}

describe('ApplicationComponent', () => {
  const events = new Subject<unknown>();
  const parseUrl = vi.fn();
  const navigate = vi.fn().mockResolvedValue(true);
  let component: ApplicationComponent;

  beforeEach(() => {
    parseUrl.mockReset();
    navigate.mockClear();
    const router = { events, url: '/application/running/app-1', parseUrl, navigate } as unknown as Router;
    component = new ApplicationComponent(activatedRoute, router, cdr);
  });

  it('reads the selected application id and running state from the url', () => {
    parseUrl.mockReturnValue(urlTree(['application', 'running', 'app-1']));

    component.ngOnInit();

    expect(component.applicationIdSelected).toBe('app-1');
    expect(component.isCompleted).toBe(false);
    expect(component.cardTitle).toBe('Running Applications');
  });

  it('recognises the completed list with no application selected', () => {
    parseUrl.mockReturnValue(urlTree(['application', 'completed']));

    component.ngOnInit();

    expect(component.applicationIdSelected).toBeUndefined();
    expect(component.isCompleted).toBe(true);
    expect(component.cardTitle).toBe('Completed Applications');
  });

  it('re-reads the url on navigation end', () => {
    parseUrl.mockReturnValue(urlTree(['application', 'running', 'app-1']));
    component.ngOnInit();

    parseUrl.mockReturnValue(urlTree(['application', 'completed', 'app-2']));
    events.next(new NavigationEnd(1, '/application/completed/app-2', '/application/completed/app-2'));

    expect(component.applicationIdSelected).toBe('app-2');
    expect(component.isCompleted).toBe(true);
  });

  it('navigates to an application relative to the current route', () => {
    component.navigateToApplication({ id: 'app-9' } as ApplicationItem);

    expect(navigate).toHaveBeenCalledWith(['app-9'], { relativeTo: activatedRoute });
  });
});
