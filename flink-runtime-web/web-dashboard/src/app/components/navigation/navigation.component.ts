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

import { NgForOf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { filter, mergeMap, map, startWith, takeUntil } from 'rxjs/operators';

import { RouterTab } from '@flink-runtime-web/core/module-config';
import { NzTabsModule } from 'ng-zorro-antd/tabs';

@Component({
  selector: 'flink-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzTabsModule, NgForOf],
  standalone: true
})
export class NavigationComponent implements OnInit, OnDestroy {
  @Input() listOfNavigation: RouterTab[] = [];
  @Input() size = 'default';
  navIndex = 0;
  destroy$ = new Subject<void>();

  navigateTo(path: string): void {
    this.router.navigate([path], { relativeTo: this.activatedRoute }).then();
  }

  constructor(private activatedRoute: ActivatedRoute, private router: Router, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.router.events
      .pipe(
        filter(e => e instanceof NavigationEnd),
        startWith(true),
        filter(() => !!(this.activatedRoute && this.activatedRoute.firstChild)),
        mergeMap(() => this.activatedRoute!.firstChild!.data),
        takeUntil(this.destroy$),
        map(data => data.path)
      )
      .subscribe(data => {
        this.navIndex = this.listOfNavigation.map(nav => nav.path).indexOf(data);
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
