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
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router, RouterOutlet } from '@angular/router';
import { Subject } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

import { JobListComponent } from '@flink-runtime-web/components/job-list/job-list.component';
import { JobsItem } from '@flink-runtime-web/interfaces';

@Component({
  selector: 'flink-job',
  templateUrl: './job.component.html',
  styleUrls: ['./job.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, JobListComponent, RouterOutlet],
  standalone: true
})
export class JobComponent implements OnInit, OnDestroy {
  jobIdSelected?: string;
  isCompleted = false;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private activatedRoute: ActivatedRoute,
    private router: Router,
    private readonly cdr: ChangeDetectorRef
  ) {}

  get cardTitle(): string {
    return this.isCompleted ? 'Completed Jobs' : 'Running Jobs';
  }

  ngOnInit(): void {
    this.updateJobIdSelected();
    this.router.events
      .pipe(
        filter(event => event instanceof NavigationEnd),
        takeUntil(this.destroy$)
      )
      .subscribe(() => {
        this.updateJobIdSelected();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  navigateToJob(job: JobsItem): void {
    this.router.navigate([job.jid], { relativeTo: this.activatedRoute }).then();
  }

  private updateJobIdSelected(): void {
    const segments = this.router.parseUrl(this.router.url).root.children.primary.segments;
    this.jobIdSelected = segments[2]?.toString();
    this.isCompleted = segments[1].path === 'completed';
    this.cdr.markForCheck();
  }
}
