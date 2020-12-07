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

import {ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {EMPTY, Subject} from 'rxjs';
import {catchError, flatMap, takeUntil} from 'rxjs/operators';
import {JobService, StatusService} from 'services';

@Component({
  selector: 'flink-job',
  templateUrl: './job.component.html',
  styleUrls: ['./job.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  isLoading = true;
  isError = false;
  errorDetails: string;

  constructor(
    private cdr: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute,
    private jobService: JobService,
    private statusService: StatusService
  ) {}

  ngOnInit() {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() =>
          this.jobService.loadJob(this.activatedRoute.snapshot.params.jid).pipe(
            catchError(() => {
              this.jobService.loadExceptions(this.activatedRoute.snapshot.params.jid, 10).subscribe(data => {
                this.errorDetails = data['root-exception'];
                this.cdr.markForCheck();
              });
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

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
