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
import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilChanged, mergeMap, takeUntil, tap } from 'rxjs/operators';

import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';
import { flinkEditorOptions } from 'share/common/editor/editor-config';

import { ExceptionInfoInterface, RootExceptionInfoInterface } from 'interfaces';
import { JobService } from 'services';

interface ExceptionHistoryItem {
  /**
   * List of concurrent exceptions that caused this failure.
   */
  exceptions: ExceptionInfoInterface[];

  /**
   * An exception from the list, that is currently selected for rendering.
   */
  selected: ExceptionInfoInterface;

  /**
   * Should this failure be expanded in UI?
   */
  expand: boolean;
}

const stripConcurrentExceptions = function(rootException: RootExceptionInfoInterface): ExceptionInfoInterface {
  const { concurrentExceptions, ...mainException } = rootException;
  return mainException;
};

const markGlobalFailure = function(exception: ExceptionInfoInterface): ExceptionInfoInterface {
  if (exception.taskName == null) {
    exception.taskName = '(global failure)';
  }
  return exception;
};

@Component({
  selector: 'flink-job-exceptions',
  templateUrl: './job-exceptions.component.html',
  styleUrls: ['./job-exceptions.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobExceptionsComponent implements OnInit, OnDestroy {
  rootException = '';
  exceptionHistory: ExceptionHistoryItem[] = [];
  truncated = false;
  isLoading = false;
  maxExceptions = 0;
  total = 0;
  editorOptions: EditorOptions = flinkEditorOptions;
  private destroy$ = new Subject<void>();

  trackExceptionBy(_: number, node: ExceptionInfoInterface): number {
    return node.timestamp;
  }
  loadMore(): void {
    this.isLoading = true;
    this.maxExceptions += 10;
    this.jobService.jobDetail$
      .pipe(
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        mergeMap(job => this.jobService.loadExceptions(job.jid, this.maxExceptions)),
        tap(() => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        // @ts-ignore
        const exceptionHistory = data.exceptionHistory;
        if (exceptionHistory.entries.length > 0) {
          const mostRecentException = exceptionHistory.entries[0];
          this.rootException = `${formatDate(mostRecentException.timestamp, 'yyyy-MM-dd HH:mm:ss', 'en')}\n${
            mostRecentException.stacktrace
          }`;
        } else {
          this.rootException = 'No Root Exception';
        }
        this.truncated = exceptionHistory.truncated;
        this.exceptionHistory = exceptionHistory.entries.map(entry => {
          const values = [stripConcurrentExceptions(entry)].concat(entry.concurrentExceptions).map(markGlobalFailure);
          return {
            selected: values[0],
            exceptions: values,
            expand: false
          };
        });
      });
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.loadMore();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
