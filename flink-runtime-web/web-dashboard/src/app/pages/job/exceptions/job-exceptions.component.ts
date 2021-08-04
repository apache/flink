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

import { formatDate } from '@angular/common';
import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { ExceptionInfoInterface } from 'interfaces';
import { distinctUntilChanged, flatMap, tap } from 'rxjs/operators';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-exceptions',
  templateUrl: './job-exceptions.component.html',
  styleUrls: ['./job-exceptions.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobExceptionsComponent implements OnInit {
  rootException = '';
  listOfException: ExceptionInfoInterface[] = [];
  truncated = false;
  isLoading = false;
  maxExceptions = 0;
  total = 0;

  trackExceptionBy(_: number, node: ExceptionInfoInterface) {
    return node.timestamp;
  }
  loadMore() {
    this.isLoading = true;
    this.maxExceptions += 10;
    this.jobService.jobDetail$
      .pipe(
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        flatMap(job => this.jobService.loadExceptions(job.jid, this.maxExceptions)),
        tap(() => {
          this.isLoading = false;
          this.cdr.markForCheck();
        })
      )
      .subscribe(data => {
        // @ts-ignore
        var exceptionHistory = data.exceptionHistory
        if (exceptionHistory.entries.length > 0) {
          var mostRecentException = exceptionHistory.entries[0]
          this.rootException = formatDate(mostRecentException.timestamp, 'yyyy-MM-dd HH:mm:ss', 'en') + '\n' + mostRecentException.stacktrace;
        } else {
          this.rootException = 'No Root Exception';
        }
        this.truncated = exceptionHistory.truncated;
        this.listOfException = exceptionHistory.entries;
      });
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.loadMore();
  }
}
