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

import { Component, EventEmitter, Input, Output, ChangeDetectionStrategy, ElementRef } from '@angular/core';
import { deepFind } from 'utils';
import { NodesItemCorrectInterface } from 'interfaces';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

@Component({
  selector: 'flink-job-overview-list',
  templateUrl: './job-overview-list.component.html',
  styleUrls: ['./job-overview-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewListComponent {
  innerNodes: NodesItemCorrectInterface[] = [];
  sortName: string;
  sortValue: string;
  left = 390;
  @Output() nodeClick = new EventEmitter();
  @Input() selectedNode: NodesItemCorrectInterface;

  @Input()
  set nodes(value: NodesItemCorrectInterface[]) {
    this.innerNodes = value;
  }

  get nodes() {
    return this.innerNodes;
  }

  sortStatusFn = this.sortFn('detail.status');
  sortReadBytesFn = this.sortFn('detail.metrics.read-bytes');
  sortReadRecordsFn = this.sortFn('detail.metrics.read-records');
  sortWriteBytesFn = this.sortFn('detail.metrics.write-bytes');
  sortWriteRecordsFn = this.sortFn('detail.metrics.write-records');
  sortParallelismFn = this.sortFn('parallelism');
  sortStartTimeFn = this.sortFn('detail.start-time');
  sortDurationFn = this.sortFn('detail.duration');
  sortEndTimeFn = this.sortFn('detail.end-time');

  sortFn(path: string): NzTableSortFn<NodesItemCorrectInterface> {
    return (pre: NodesItemCorrectInterface, next: NodesItemCorrectInterface) =>
      deepFind(pre, path) > deepFind(next, path) ? 1 : -1;
  }

  trackJobBy(_: number, node: NodesItemCorrectInterface) {
    return node.id;
  }

  clickNode(node: NodesItemCorrectInterface) {
    this.nodeClick.emit(node);
  }

  constructor(public elementRef: ElementRef) {}
}
