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
import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  Output,
  SimpleChanges
} from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { mergeMap, takeUntil } from 'rxjs/operators';

import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobBadgeComponent } from '@flink-runtime-web/components/job-badge/job-badge.component';
import { TaskBadgeComponent } from '@flink-runtime-web/components/task-badge/task-badge.component';
import { JobsItem } from '@flink-runtime-web/interfaces';
import { JobService, StatusService } from '@flink-runtime-web/services';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzTableModule } from 'ng-zorro-antd/table';

@Component({
  selector: 'flink-job-list',
  templateUrl: './job-list.component.html',
  styleUrls: ['./job-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzCardModule,
    NzTableModule,
    TaskBadgeComponent,
    JobBadgeComponent,
    NgForOf,
    HumanizeDatePipe,
    HumanizeDurationPipe,
    NzMessageModule
  ],
  standalone: true
})
export class JobListComponent implements OnInit, OnDestroy, OnChanges {
  listOfJob: JobsItem[] = [];
  isLoading = true;
  destroy$ = new Subject<void>();
  @Input() completed = false;
  @Input() title: string;
  @Input() jobData$: Observable<JobsItem[]>;
  @Output() navigate = new EventEmitter<JobsItem>();

  sortJobNameFn = (pre: JobsItem, next: JobsItem): number => pre.name.localeCompare(next.name);
  sortStartTimeFn = (pre: JobsItem, next: JobsItem): number => pre['start-time'] - next['start-time'];
  sortDurationFn = (pre: JobsItem, next: JobsItem): number => pre.duration - next.duration;
  sortEndTimeFn = (pre: JobsItem, next: JobsItem): number => pre['end-time'] - next['end-time'];
  sortStateFn = (pre: JobsItem, next: JobsItem): number => pre.state.localeCompare(next.state);

  trackJobBy(_: number, node: JobsItem): string {
    return node.jid;
  }

  navigateToJob(job: JobsItem): void {
    if (job.state === 'INITIALIZING') {
      this.nzMessageService.info('Job detail page is not available while it is in state INITIALIZING.');
    } else {
      this.navigate.emit(job);
    }
  }

  constructor(
    private statusService: StatusService,
    private jobService: JobService,
    private nzMessageService: NzMessageService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.jobData$ =
      this.jobData$ ||
      this.statusService.refresh$.pipe(
        takeUntil(this.destroy$),
        mergeMap(() => this.jobService.loadJobs())
      );
    this.jobData$.subscribe(data => {
      this.isLoading = false;
      this.listOfJob = data.filter(item => item.completed === this.completed);
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  ngOnChanges(changes: SimpleChanges): void {
    const { completed } = changes;
    if (completed) {
      this.isLoading = true;
      this.cdr.markForCheck();
    }
  }
}
