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

import { DecimalPipe, NgForOf, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, Component, ElementRef, EventEmitter, Input, Output } from '@angular/core';

import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobBadgeComponent } from '@flink-runtime-web/components/job-badge/job-badge.component';
import { ResizeComponent } from '@flink-runtime-web/components/resize/resize.component';
import { TaskBadgeComponent } from '@flink-runtime-web/components/task-badge/task-badge.component';
import { NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

function createSortFn(
  selector: (item: NodesItemCorrect) => number | string | undefined
): NzTableSortFn<NodesItemCorrect> {
  return (pre, next) => (selector(pre)! > selector(next)! ? 1 : -1);
}

@Component({
  selector: 'flink-job-overview-list',
  templateUrl: './job-overview-list.component.html',
  styleUrls: ['./job-overview-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzTableModule,
    NgForOf,
    NzToolTipModule,
    JobBadgeComponent,
    NgIf,
    HumanizeBytesPipe,
    DecimalPipe,
    HumanizeDatePipe,
    HumanizeDurationPipe,
    TaskBadgeComponent,
    ResizeComponent
  ],
  standalone: true
})
export class JobOverviewListComponent {
  public readonly trackById = (_: number, node: NodesItemCorrect): string => node.id;

  public readonly sortStatusFn = createSortFn(item => item.detail?.status);
  public readonly sortReadBytesFn = createSortFn(item => item.detail?.metrics?.['read-bytes']);
  public readonly sortReadRecordsFn = createSortFn(item => item.detail?.metrics?.['read-records']);
  public readonly sortWriteBytesFn = createSortFn(item => item.detail?.metrics?.['write-bytes']);
  public readonly sortWriteRecordsFn = createSortFn(item => item.detail?.metrics?.['write-records']);
  public readonly sortParallelismFn = createSortFn(item => item.parallelism);
  public readonly sortStartTimeFn = createSortFn(item => item.detail?.['start-time']);
  public readonly sortDurationFn = createSortFn(item => item.detail?.duration);
  public readonly sortEndTimeFn = createSortFn(item => item.detail?.['end-time']);

  public innerNodes: NodesItemCorrect[] = [];
  public sortName: string;
  public sortValue: string;
  public left = 390;

  @Output() public readonly nodeClick = new EventEmitter<NodesItemCorrect>();

  @Input() public selectedNode: NodesItemCorrect;

  @Input()
  public set nodes(value: NodesItemCorrect[]) {
    this.innerNodes = value;
  }

  public get nodes(): NodesItemCorrect[] {
    return this.innerNodes;
  }

  constructor(public readonly elementRef: ElementRef) {}

  public clickNode(node: NodesItemCorrect): void {
    this.nodeClick.emit(node);
  }
}
