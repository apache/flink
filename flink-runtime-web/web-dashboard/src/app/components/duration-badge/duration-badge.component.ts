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

import { Component, ChangeDetectionStrategy, Input } from '@angular/core';

import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { ColorKey, ConfigService } from '@flink-runtime-web/services';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'flink-duration-badge',
  templateUrl: './duration-badge.component.html',
  styleUrls: ['./duration-badge.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[style.backgroundColor]': 'backgroundColor'
  },
  imports: [NzToolTipModule, HumanizeDurationPipe],
  standalone: true
})
export class DurationBadgeComponent {
  @Input() public state: string;
  @Input() public duration: number;

  constructor(private readonly configService: ConfigService) {}

  get backgroundColor(): string {
    return this.configService.COLOR_MAP[this.state as ColorKey];
  }
}
