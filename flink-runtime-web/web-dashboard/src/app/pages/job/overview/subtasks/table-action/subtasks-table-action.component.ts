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

import { Component, ChangeDetectionStrategy, Input, ChangeDetectorRef, OnInit } from '@angular/core';

import { JobVertexStatusDuration, JobVertexSubTask } from '@flink-runtime-web/interfaces';

export interface JobOverviewSubtasksTableAction {
  subtask?: JobVertexSubTask;
}

@Component({
  selector: 'flink-table-actions',
  templateUrl: './subtasks-table-action.component.html',
  styleUrls: ['./subtasks-table-action.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class SubtasksTableActionComponent implements OnInit, JobOverviewSubtasksTableAction {
  @Input() subtask?: JobVertexSubTask;
  statusDuration: Array<{ key: string; value: number }> = [];
  visible = false;

  constructor(private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    if (this.subtask) {
      this.statusDuration = this.convertStatusDuration(this.subtask['status-duration']);
    }
  }

  setModalVisible(visible: boolean): void {
    this.visible = visible;
    this.cdr.markForCheck();
  }

  convertStatusDuration(duration: JobVertexStatusDuration<number>): Array<{ key: string; value: number }> {
    return Object.keys(duration).map(key => ({
      key,
      value: duration[key as keyof JobVertexStatusDuration<number>]
    }));
  }
}
