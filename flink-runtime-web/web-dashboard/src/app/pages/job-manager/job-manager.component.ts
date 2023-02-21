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

import { ChangeDetectionStrategy, Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';

import { NavigationComponent } from '@flink-runtime-web/components/navigation/navigation.component';

@Component({
  selector: 'flink-job-manager',
  templateUrl: './job-manager.component.html',
  styleUrls: ['./job-manager.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NavigationComponent, RouterOutlet],
  standalone: true
})
export class JobManagerComponent {
  public readonly listOfNavigation = [
    { path: 'metrics', title: 'Metrics' },
    { path: 'config', title: 'Configuration' },
    { path: 'logs', title: 'Logs' },
    { path: 'stdout', title: 'Stdout' },
    { path: 'log', title: 'Log List' },
    { path: 'thread-dump', title: 'Thread Dump' }
  ];
}
