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
import { takeUntil } from 'rxjs/operators';

import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';
import { flinkEditorOptions } from 'share/common/editor/editor-config';

import { JobManagerService } from 'services';

@Component({
  selector: 'flink-job-manager-stdout',
  templateUrl: './job-manager-stdout.component.html',
  styleUrls: ['./job-manager-stdout.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobManagerStdoutComponent implements OnInit, OnDestroy {
  public readonly editorOptions: EditorOptions = flinkEditorOptions;

  public stdout = '';
  public loading = true;

  private readonly destroy$ = new Subject<void>();

  constructor(private readonly jobManagerService: JobManagerService, private readonly cdr: ChangeDetectorRef) {}

  public ngOnInit(): void {
    this.reload();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reload(): void {
    this.loading = true;
    this.cdr.markForCheck();
    this.jobManagerService
      .loadStdout()
      .pipe(takeUntil(this.destroy$))
      .subscribe(data => {
        this.loading = false;
        this.stdout = data;
        this.cdr.markForCheck();
      });
  }
}
