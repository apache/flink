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

import { NgForOf, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, Component, Input } from '@angular/core';

import { JobStatusCounts, TaskStatus } from '@flink-runtime-web/interfaces';
import { ConfigService } from '@flink-runtime-web/services';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'flink-task-badge',
  templateUrl: './task-badge.component.html',
  styleUrls: ['./task-badge.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgForOf, NgIf, NzToolTipModule],
  standalone: true
})
export class TaskBadgeComponent {
  @Input() tasks: TaskStatus | JobStatusCounts;
  statusList = Object.keys(this.configService.COLOR_MAP);

  constructor(private readonly configService: ConfigService) {}

  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  get colorMap() {
    return this.configService.COLOR_MAP;
  }
}
