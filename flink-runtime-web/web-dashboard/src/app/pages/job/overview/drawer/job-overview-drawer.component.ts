/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { animate, style, transition, trigger } from '@angular/animations';
import { Component, OnInit, ChangeDetectionStrategy, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { JobService } from 'flink-services';
import { combineLatest, Subject } from 'rxjs';
import { filter, map, takeUntil } from 'rxjs/operators';

@Component({
  selector       : 'flink-job-overview-drawer',
  templateUrl    : './job-overview-drawer.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  animations     : [
    trigger('drawer', [
      transition('void => *', [
        style({ transform: 'translateX(100%)' }),
        animate(100, style({ transform: 'translateX(0)' }))
      ]),
      transition('* => void', [
        style({ transform: 'translateX(0)' }),
        animate(100, style({ transform: 'translateX(100%)' }))
      ])
    ])
  ],
  host           : {
    '[@drawer]'         : '',
    '[class.full-width]': 'fullScreen'
  },
  styleUrls      : [ './job-overview-drawer.component.less' ]
})
export class JobOverviewDrawerComponent implements OnInit, OnDestroy {
  cachePath = this.jobService.listOfNavigation[ 0 ].pathOrParam;
  destroy$ = new Subject();
  listOfNavigation = this.jobService.listOfNavigation;
  fullScreen = false;

  rightDrawer() {
    if (this.fullScreen) {
      this.fullScreen = false;
    } else {
      this.router.navigate([ '../../' ], { relativeTo: this.activatedRoute }).then();
      this.jobService.selectedVertexNode$.next(null);
    }
  }


  leftDrawer() {
    this.fullScreen = true;
  }

  constructor(private activatedRoute: ActivatedRoute, private router: Router, private jobService: JobService) {
  }

  ngOnInit() {
    const nodeId$ = this.activatedRoute.params.pipe(map(item => item.vertexId));
    combineLatest(
      this.jobService.jobDetail$.pipe(map(item => item.plan.nodes)),
      nodeId$
    ).pipe(takeUntil(this.destroy$)).subscribe(data => {
      if (!this.activatedRoute.firstChild) {
        this.router.navigate([ this.cachePath ], { relativeTo: this.activatedRoute });
      } else {
        this.cachePath = this.activatedRoute.firstChild.snapshot.data.path;
      }
      if (data[ 0 ] && data[ 1 ]) {
        this.jobService.selectedVertexNode$.next(data[ 0 ].find(item => item.id === data[ 1 ]));
      }
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

}
