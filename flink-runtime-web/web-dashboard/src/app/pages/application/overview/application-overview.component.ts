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

import { ChangeDetectionStrategy, Component, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Observable, shareReplay, Subject } from 'rxjs';
import { map, takeUntil } from 'rxjs/operators';

import { JobListComponent } from '@flink-runtime-web/components/job-list/job-list.component';
import { JobsItem } from '@flink-runtime-web/interfaces';
import { ApplicationLocalService } from '@flink-runtime-web/pages/application/application-local.service';

@Component({
  selector: 'application-overview',
  templateUrl: './application-overview.component.html',
  styleUrls: ['./application-overview.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [JobListComponent]
})
export class ApplicationOverviewComponent implements OnInit, OnDestroy {
  public jobData$: Observable<JobsItem[]>;

  private readonly destroy$ = new Subject<void>();

  constructor(private readonly applicationLocalService: ApplicationLocalService, private router: Router) {}

  public ngOnInit(): void {
    this.jobData$ = this.applicationLocalService.applicationDetailChanges().pipe(
      takeUntil(this.destroy$),
      map(data => data.jobs),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public navigateToJob(commands: string[]): void {
    this.router.navigate(commands).then();
  }
}
