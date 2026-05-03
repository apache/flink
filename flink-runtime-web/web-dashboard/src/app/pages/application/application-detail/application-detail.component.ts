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

import { NgIf } from '@angular/common';
import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy } from '@angular/core';
import { ActivatedRoute, RouterOutlet } from '@angular/router';
import { EMPTY, Subject } from 'rxjs';
import { catchError, mergeMap, takeUntil, tap } from 'rxjs/operators';

import { ApplicationStatusComponent } from '@flink-runtime-web/pages/application/application-detail/status/application-status.component';
import { ApplicationLocalService } from '@flink-runtime-web/pages/application/application-local.service';
import { ApplicationService, StatusService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';

@Component({
  selector: 'flink-application-detail',
  templateUrl: './application-detail.component.html',
  styleUrls: ['./application-detail.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, ApplicationStatusComponent, NzSkeletonModule, RouterOutlet, NzAlertModule]
})
export class ApplicationDetailComponent implements OnInit, OnDestroy {
  isLoading = true;
  isError = false;
  errorDetails: string;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly applicationService: ApplicationService,
    private readonly applicationLocalService: ApplicationLocalService,
    private readonly statusService: StatusService,
    private activatedRoute: ActivatedRoute,
    private readonly cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        mergeMap(() =>
          this.applicationService.loadApplication(this.activatedRoute.snapshot.params['id']).pipe(
            tap(application => {
              this.applicationLocalService.setApplicationDetail(application);
            }),
            catchError(() => {
              this.isError = true;
              this.isLoading = false;
              this.cdr.markForCheck();
              return EMPTY;
            })
          )
        )
      )
      .subscribe(() => {
        this.isLoading = false;
        this.isError = false;
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
