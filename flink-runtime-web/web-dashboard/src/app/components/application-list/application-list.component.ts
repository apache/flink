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

import { ApplicationBadgeComponent } from '@flink-runtime-web/components/application-badge/application-badge.component';
import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobsBadgeComponent } from '@flink-runtime-web/components/jobs-badge/jobs-badge.component';
import { ApplicationItem } from '@flink-runtime-web/interfaces';
import { ApplicationService, StatusService } from '@flink-runtime-web/services';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTableModule } from 'ng-zorro-antd/table';

@Component({
  selector: 'flink-application-list',
  templateUrl: './application-list.component.html',
  styleUrls: ['./application-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzCardModule,
    NzTableModule,
    JobsBadgeComponent,
    ApplicationBadgeComponent,
    NgForOf,
    HumanizeDatePipe,
    HumanizeDurationPipe
  ]
})
export class ApplicationListComponent implements OnInit, OnDestroy, OnChanges {
  listOfApplication: ApplicationItem[] = [];
  isLoading = true;
  destroy$ = new Subject<void>();
  @Input() completed = false;
  @Input() title: string;
  @Input() applicationData$: Observable<ApplicationItem[]>;
  @Output() navigate = new EventEmitter<ApplicationItem>();

  sortApplicationNameFn = (pre: ApplicationItem, next: ApplicationItem): number => pre.name.localeCompare(next.name);
  sortStartTimeFn = (pre: ApplicationItem, next: ApplicationItem): number => pre['start-time'] - next['start-time'];
  sortDurationFn = (pre: ApplicationItem, next: ApplicationItem): number => pre.duration - next.duration;
  sortEndTimeFn = (pre: ApplicationItem, next: ApplicationItem): number => pre['end-time'] - next['end-time'];
  sortStateFn = (pre: ApplicationItem, next: ApplicationItem): number => pre.status.localeCompare(next.status);

  trackApplicationBy(_: number, node: ApplicationItem): string {
    return node.id;
  }

  navigateToApplication(application: ApplicationItem): void {
    this.navigate.emit(application);
  }

  constructor(
    private statusService: StatusService,
    private applicationService: ApplicationService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.applicationData$ =
      this.applicationData$ ||
      this.statusService.refresh$.pipe(
        takeUntil(this.destroy$),
        mergeMap(() => this.applicationService.loadApplications())
      );
    this.applicationData$.subscribe(data => {
      this.isLoading = false;
      this.listOfApplication = data.filter(item => item.completed === this.completed);

      // Apply default sorting based on completed status
      if (this.completed) {
        // Sort completed applications by end time (descending - most recent first)
        this.listOfApplication.sort((a, b) => b['end-time'] - a['end-time']);
      } else {
        // Sort running applications by start time (descending - most recent first)
        this.listOfApplication.sort((a, b) => b['start-time'] - a['start-time']);
      }

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
