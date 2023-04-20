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

import { NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input } from '@angular/core';

import { NodesItemCorrect } from '@flink-runtime-web/interfaces';

@Component({
  selector: '[flink-node]',
  templateUrl: './node.component.html',
  styleUrls: ['./node.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf],
  standalone: true
})
export class NodeComponent {
  description: string | null;
  operator: string | null;
  operatorStrategy: string | null;
  parallelism: number | null;
  lowWatermark: number | null | undefined;
  backPressuredPercentage: number | undefined = NaN;
  busyPercentage: number | undefined = NaN;
  backgroundColor: string | undefined;
  borderColor: string | undefined;
  height = 0;
  id: string;
  backgroundBusyColor = '#ee6464';
  backgroundDefaultColor = '#5db1ff';
  backgroundBackPressuredColor = '#888888';
  borderBusyColor = '#ee2222';
  borderDefaultColor = '#1890ff';
  borderBackPressuredColor = '#000000';

  decodeHTML(value: string): string | null {
    const parser = new DOMParser();
    const dom = parser.parseFromString(`<!doctype html><body>${value}`, 'text/html');
    return dom.body.textContent;
  }

  @Input()
  set node(value: NodesItemCorrect) {
    let description = this.decodeHTML(value.description);
    if (value.detail) {
      description = this.decodeHTML(value.detail.name);
    }
    this.operator = this.decodeHTML(value.operator);
    this.operatorStrategy = this.decodeHTML(value.operator_strategy);
    this.parallelism = value.parallelism;
    this.lowWatermark = value.lowWatermark;
    if (this.isValid(value.backPressuredPercentage)) {
      this.backPressuredPercentage = value.backPressuredPercentage;
    }
    if (this.isValid(value.busyPercentage)) {
      this.busyPercentage = value.busyPercentage;
    }
    this.height = value.height || 0;
    this.id = value.id;
    if (description && description.length > 300) {
      this.description = `${description.slice(0, 300)}...`;
    } else {
      this.description = description;
    }
  }

  isValid = (value?: number): boolean => {
    return !!value || value === 0 || value === NaN;
  };

  toRGBA = (d: string): number[] => {
    const l = d.length;
    const rgba = [];
    const hex = parseInt(d.slice(1), 16);
    rgba[0] = (hex >> 16) & 255;
    rgba[1] = (hex >> 8) & 255;
    rgba[2] = hex & 255;
    rgba[3] = l === 9 || l === 5 ? Math.round((((hex >> 24) & 255) / 255) * 10000) / 10000 : -1;
    return rgba;
  };

  blend = (from: string, to: string, p = 0.5): string => {
    from = from.trim();
    to = to.trim();
    const b = p < 0;
    p = b ? p * -1 : p;
    const f = this.toRGBA(from);
    const t = this.toRGBA(to);
    if (to[0] === 'r') {
      return `rgb${to[3] === 'a' ? 'a(' : '('}${Math.round((t[0] - f[0]) * p + f[0])},${Math.round(
        (t[1] - f[1]) * p + f[1]
      )},${Math.round((t[2] - f[2]) * p + f[2])}${
        f[3] < 0 && t[3] < 0
          ? ''
          : `,${
              f[3] > -1 && t[3] > -1 ? Math.round(((t[3] - f[3]) * p + f[3]) * 10000) / 10000 : t[3] < 0 ? f[3] : t[3]
            }`
      })`;
    }

    return `#${(
      0x100000000 +
      (f[3] > -1 && t[3] > -1
        ? Math.round(((t[3] - f[3]) * p + f[3]) * 255)
        : t[3] > -1
        ? Math.round(t[3] * 255)
        : f[3] > -1
        ? Math.round(f[3] * 255)
        : 255) *
        0x1000000 +
      Math.round((t[0] - f[0]) * p + f[0]) * 0x10000 +
      Math.round((t[1] - f[1]) * p + f[1]) * 0x100 +
      Math.round((t[2] - f[2]) * p + f[2])
    )
      .toString(16)
      .slice(f[3] > -1 || t[3] > -1 ? 1 : 3)}`;
  };

  constructor(protected cd: ChangeDetectorRef) {}

  /**
   * Update and check node component
   *
   * @param node
   */
  update(node: NodesItemCorrect): void {
    this.node = node;
    this.backgroundColor = this.backgroundDefaultColor;
    this.borderColor = this.borderDefaultColor;
    if (node.busyPercentage) {
      this.backgroundColor = this.blend(this.backgroundColor, this.backgroundBusyColor, node.busyPercentage / 100.0);
      this.borderColor = this.blend(this.borderColor, this.borderBusyColor, node.busyPercentage / 100.0);
    }
    if (node.backPressuredPercentage) {
      this.backgroundColor = this.blend(
        this.backgroundColor,
        this.backgroundBackPressuredColor,
        node.backPressuredPercentage / 100.0
      );
      this.borderColor = this.blend(
        this.borderColor,
        this.borderBackPressuredColor,
        node.backPressuredPercentage / 100.0
      );
    }
    this.cd.markForCheck();
  }

  prettyPrint(value: number | undefined): string {
    if (value === undefined || isNaN(value)) {
      return 'N/A';
    } else {
      return `${value}%`;
    }
  }
}
