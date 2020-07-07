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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { filter, flatMap, map, startWith, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'flink-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NavigationComponent implements OnInit, OnDestroy {
  @Input() listOfNavigation: Array<{ path: string; title: string }> = [];
  @Input() tabBarGutter = 8;
  @Input() size = 'default';
  navIndex = 0;
  destroy$ = new Subject();

  navigateTo(path: string) {
    this.router.navigate([path], { relativeTo: this.activatedRoute }).then();
  }

  constructor(private activatedRoute: ActivatedRoute, private router: Router, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.router.events
      .pipe(
        filter(e => e instanceof NavigationEnd),
        startWith(true),
        filter(() => !!(this.activatedRoute && this.activatedRoute.firstChild)),
        flatMap(() => this.activatedRoute!.firstChild!.data),
        takeUntil(this.destroy$),
        map(data => data.path)
      )
      .subscribe(data => {
        this.navIndex = this.listOfNavigation.map(nav => nav.path).indexOf(data);
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
