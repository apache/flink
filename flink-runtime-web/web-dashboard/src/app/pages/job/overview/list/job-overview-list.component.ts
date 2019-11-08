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
    this.search();
  }

  get nodes() {
    return this.innerNodes;
  }

  sort(sort: { key: string; value: string }) {
    this.sortName = sort.key;
    this.sortValue = sort.value;
    this.search();
  }

  search() {
    if (this.sortName) {
      this.innerNodes = [
        ...this.innerNodes.sort((pre, next) => {
          if (this.sortValue === 'ascend') {
            return deepFind(pre, this.sortName) > deepFind(next, this.sortName) ? 1 : -1;
          } else {
            return deepFind(next, this.sortName) > deepFind(pre, this.sortName) ? 1 : -1;
          }
        })
      ];
    }
  }

  trackJobBy(_: number, node: NodesItemCorrectInterface) {
    return node.id;
  }

  clickNode(node: NodesItemCorrectInterface) {
    this.nodeClick.emit(node);
  }

  constructor(public elementRef: ElementRef) {}
}
