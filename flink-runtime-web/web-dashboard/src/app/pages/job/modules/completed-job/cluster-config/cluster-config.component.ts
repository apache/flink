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

import { Component, OnInit, ChangeDetectionStrategy, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, of, Subject } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';

import { ConfigurationCardsComponent } from '@flink-runtime-web/components/configuration-cards/configuration-cards.component';
import { ClusterConfiguration, EnvironmentInfo } from '@flink-runtime-web/interfaces';
import { JobManagerService } from '@flink-runtime-web/services';

@Component({
  selector: 'flink-cluster-config',
  templateUrl: './cluster-config.component.html',
  styleUrls: ['./cluster-config.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [ConfigurationCardsComponent],
  standalone: true
})
export class ClusterConfigComponent implements OnInit, OnDestroy {
  jobId: string;
  configurations: ClusterConfiguration[] = [];
  environmentInfo?: EnvironmentInfo;
  loading = true;
  private destroy$ = new Subject<void>();

  constructor(
    private activatedRoute: ActivatedRoute,
    private jobManagerService: JobManagerService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.jobId = this.activatedRoute.parent!.snapshot.params.jid;
    forkJoin([
      this.jobManagerService
        .loadHistoryServerConfig(this.jobId)
        .pipe(catchError(() => of([] as ClusterConfiguration[]))),
      this.jobManagerService.loadHistoryServerEnvironment(this.jobId).pipe(catchError(() => of(undefined)))
    ])
      .pipe(takeUntil(this.destroy$))
      .subscribe(([config, env]) => {
        this.loading = false;
        this.configurations = config.sort((pre, next) => (pre.key > next.key ? 1 : -1));
        this.environmentInfo = env;
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
