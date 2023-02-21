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
import { Component, ChangeDetectionStrategy, Input, ChangeDetectorRef } from '@angular/core';
import { RouterLinkWithHref } from '@angular/router';

import { TableAggregatedMetricsComponent } from '@flink-runtime-web/components/table-aggregated-metrics/table-aggregated-metrics.component';
import { VertexTaskManagerDetail } from '@flink-runtime-web/interfaces';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzModalModule } from 'ng-zorro-antd/modal';

export interface JobOverviewTaskManagersTableAction {
  taskManager?: VertexTaskManagerDetail;
}

@Component({
  selector: 'flink-taskmanagers-table-action',
  templateUrl: './taskmanagers-table-action.component.html',
  styleUrls: ['./taskmanagers-table-action.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, NzDropDownModule, NzIconModule, NzModalModule, TableAggregatedMetricsComponent, RouterLinkWithHref],
  standalone: true
})
export class TaskmanagersTableActionComponent implements JobOverviewTaskManagersTableAction {
  @Input() taskManager?: VertexTaskManagerDetail;
  visible = false;

  constructor(private cdr: ChangeDetectorRef) {}

  setModalVisible(visible: boolean): void {
    this.visible = visible;
    this.cdr.markForCheck();
  }
}
