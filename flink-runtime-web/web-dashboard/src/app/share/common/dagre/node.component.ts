/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  HostListener,
  Input
} from '@angular/core';
import { NodesItemCorrectInterface } from 'flink-interfaces';
import { isNil } from 'lodash';

@Component({
  selector       : '[flink-node]',
  templateUrl    : './node.component.html',
  styleUrls      : [ './node.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NodeComponent {
  _node = {} as NodesItemCorrectInterface;
  visible = false;
  description = '';
  operator = '';
  operator_strategy = '';

  decodeHTML(value: string) {
    const parser = new DOMParser;
    const dom = parser.parseFromString('<!doctype html><body>' + value, 'text/html');
    return dom.body.textContent;
  }

  @Input()
  set node(value) {
    this._node = value;
    const description = this.decodeHTML(this.node.description);
    this.operator = this.decodeHTML(this.node.operator);
    this.operator_strategy = this.decodeHTML(this.node.operator_strategy);
    if (description.length > 300) {
      this.description = description.slice(0, 300) + '...';
    } else {
      this.description = description;
    }
  }

  get node() {
    return this._node;
  }

  @HostListener('click')
  clickNode() {
    this.visible = false;
  }

  get id() {
    return this.node.id;
  }

  get name() {
    return this.node.description;
  }

  get parallelism() {
    return this.node.parallelism;
  }

  constructor(protected cd: ChangeDetectorRef) {
  }

  update(node): void {
    this.node = node;
    this.cd.markForCheck();
  }
}
