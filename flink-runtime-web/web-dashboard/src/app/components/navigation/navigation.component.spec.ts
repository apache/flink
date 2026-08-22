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
import { Observable, of, Subject } from 'rxjs';

import { RouterTab } from '@flink-runtime-web/core/module-config';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { NavigationComponent } from './navigation.component';

describe('NavigationComponent', () => {
  const listOfNavigation: RouterTab[] = [
    { path: 'overview', title: 'Overview' },
    { path: 'exceptions', title: 'Exceptions' },
    { path: 'checkpoints', title: 'Checkpoints' }
  ];

  let activatedRoute: { firstChild: { data: Observable<{ path: string }> } | undefined };
  let routerEvents: Subject<NavigationEnd>;
  let navigate: ReturnType<typeof vi.fn>;
  let cdr: { markForCheck: ReturnType<typeof vi.fn> };
  let component: NavigationComponent;

  beforeEach(() => {
    activatedRoute = { firstChild: { data: of({ path: 'checkpoints' }) } };
    routerEvents = new Subject<NavigationEnd>();
    navigate = vi.fn().mockResolvedValue(true);
    cdr = { markForCheck: vi.fn() };
    component = new NavigationComponent(
      activatedRoute as unknown as ActivatedRoute,
      { events: routerEvents.asObservable(), navigate } as unknown as Router,
      cdr as unknown as ChangeDetectorRef
    );
    component.listOfNavigation = listOfNavigation;
  });

  it('selects the tab matching the initially active child route without waiting for a navigation event', () => {
    component.ngOnInit();

    expect(component.navIndex).toBe(2);
    expect(cdr.markForCheck).toHaveBeenCalled();
  });

  it('leaves navIndex untouched when there is no active child route yet', () => {
    activatedRoute.firstChild = undefined;

    component.ngOnInit();

    expect(component.navIndex).toBe(0);
    expect(cdr.markForCheck).not.toHaveBeenCalled();
  });

  it('re-selects the tab when a subsequent navigation activates a different child route', () => {
    component.ngOnInit();
    expect(component.navIndex).toBe(2);

    activatedRoute.firstChild = { data: of({ path: 'exceptions' }) };
    routerEvents.next(new NavigationEnd(1, '/exceptions', '/exceptions'));

    expect(component.navIndex).toBe(1);
  });

  it('stops reacting to navigation events once destroyed', () => {
    component.ngOnInit();
    component.ngOnDestroy();

    activatedRoute.firstChild = { data: of({ path: 'exceptions' }) };
    routerEvents.next(new NavigationEnd(1, '/exceptions', '/exceptions'));

    expect(component.navIndex).toBe(2);
  });

  it('navigates relative to the current route when a tab is selected', () => {
    component.navigateTo('exceptions');

    expect(navigate).toHaveBeenCalledWith(['exceptions'], { relativeTo: activatedRoute });
  });
});
