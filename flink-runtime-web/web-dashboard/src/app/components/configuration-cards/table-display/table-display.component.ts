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

import { NgForOf } from '@angular/common';
import { Component, ChangeDetectionStrategy, Input } from '@angular/core';

import { ClusterConfiguration } from '@flink-runtime-web/interfaces';
import { NzTableModule } from 'ng-zorro-antd/table';

@Component({
  selector: 'flink-table-display',
  templateUrl: './table-display.component.html',
  styleUrls: ['./table-display.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzTableModule, NgForOf],
  standalone: true
})
export class TableDisplayComponent {
  @Input() listOfData: Array<{ key: string; value: string }> = [];

  readonly trackByKey = (_: number, node: ClusterConfiguration): string => node.key;

  constructor() {}
}
