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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { TaskManagerDetailInterface } from 'interfaces';
import { first } from 'rxjs/operators';
import { TaskManagerService } from 'services';
import { MonacoEditorComponent } from 'share/common/monaco-editor/monaco-editor.component';

@Component({
  selector: 'flink-task-manager-stdout',
  templateUrl: './task-manager-stdout.component.html',
  styleUrls: ['./task-manager-stdout.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerStdoutComponent implements OnInit {
  @ViewChild(MonacoEditorComponent) monacoEditorComponent: MonacoEditorComponent;
  stdout = '';
  taskManagerDetail: TaskManagerDetailInterface;

  reload() {
    if (this.taskManagerDetail) {
      this.taskManagerService.loadStdout(this.taskManagerDetail.id).subscribe(
        data => {
          this.monacoEditorComponent.layout();
          this.stdout = data;
          this.cdr.markForCheck();
        },
        () => {
          this.cdr.markForCheck();
        }
      );
    }
  }

  constructor(private taskManagerService: TaskManagerService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.taskManagerService.taskManagerDetail$.pipe(first()).subscribe(data => {
      this.taskManagerDetail = data;
      this.reload();
      this.cdr.markForCheck();
    });
  }
}
