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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input } from '@angular/core';
import { NodesItemCorrectInterface } from 'interfaces';

@Component({
  selector: '[flink-node]',
  templateUrl: './node.component.html',
  styleUrls: ['./node.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class NodeComponent {
  description: string | null;
  operator: string | null;
  operatorStrategy: string | null;
  parallelism: number | null;
  height = 0;
  id: string;

  decodeHTML(value: string): string | null {
    const parser = new DOMParser();
    const dom = parser.parseFromString('<!doctype html><body>' + value, 'text/html');
    return dom.body.textContent;
  }

  @Input()
  set node(value: NodesItemCorrectInterface) {
    const description = this.decodeHTML(value.description);
    this.operator = this.decodeHTML(value.operator);
    this.operatorStrategy = this.decodeHTML(value.operator_strategy);
    this.parallelism = value.parallelism;
    this.height = value.height || 0;
    this.id = value.id;
    if (description && description.length > 300) {
      this.description = description.slice(0, 300) + '...';
    } else {
      this.description = description;
    }
  }

  constructor(protected cd: ChangeDetectorRef) {}

  /**
   * Update and check node component
   * @param node
   */
  update(node: NodesItemCorrectInterface): void {
    this.node = node;
    this.cd.markForCheck();
  }
}
