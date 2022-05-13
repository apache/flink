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

import { ChangeDetectorRef, Component, OnInit, ChangeDetectionStrategy, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { first, takeUntil } from 'rxjs/operators';

import { TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { TaskManagerService } from '@flink-runtime-web/services';
import { flinkEditorOptions } from '@flink-runtime-web/share/common/editor/editor-config';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';

import { TaskManagerLocalService } from '../task-manager-local.service';

@Component({
  selector: 'flink-task-manager-thread-dump',
  templateUrl: './task-manager-thread-dump.component.html',
  styleUrls: ['./task-manager-thread-dump.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerThreadDumpComponent implements OnInit, OnDestroy {
  public readonly editorOptions: EditorOptions = flinkEditorOptions;

  public dump = '';
  public loading = true;
  public taskManagerDetail: TaskManagerDetail;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly taskManagerLocalService: TaskManagerLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.taskManagerLocalService
      .taskManagerDetailChanges()
      .pipe(first(), takeUntil(this.destroy$))
      .subscribe(data => {
        this.taskManagerDetail = data;
        this.reload();
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reload(): void {
    this.loading = true;
    this.cdr.markForCheck();
    if (this.taskManagerDetail) {
      this.taskManagerService
        .loadThreadDump(this.taskManagerDetail.id)
        .pipe(takeUntil(this.destroy$))
        .subscribe(
          data => {
            this.loading = false;
            this.dump = data;
            this.cdr.markForCheck();
          },
          () => {
            this.loading = false;
            this.cdr.markForCheck();
          }
        );
    }
  }
}
