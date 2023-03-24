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
import { Component, ChangeDetectionStrategy, Input } from '@angular/core';

import { TableDisplayComponent } from '@flink-runtime-web/components/configuration-cards/table-display/table-display.component';
import { ClusterConfiguration, EnvironmentInfo, JvmInfo } from '@flink-runtime-web/interfaces';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzTableModule } from 'ng-zorro-antd/table';

@Component({
  selector: 'flink-configuration-cards',
  templateUrl: './configuration-cards.component.html',
  styleUrls: ['./configuration-cards.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzCardModule, NgIf, NzTableModule, NgForOf, NzEmptyModule, TableDisplayComponent],
  standalone: true
})
export class ConfigurationCardsComponent {
  @Input() title: string = 'Configurations';
  @Input() configurations: ClusterConfiguration[] = [];
  @Input() environmentInfo?: EnvironmentInfo;
  @Input() loading = true;

  constructor() {}

  convertJVMToKV(jvm: JvmInfo): Array<{ key: string; value: string }> {
    return [
      {
        key: 'version',
        value: jvm.version
      },
      {
        key: 'arch',
        value: jvm.arch
      },
      {
        key: 'options',
        value: jvm.options.join('\n')
      }
    ];
  }
}
