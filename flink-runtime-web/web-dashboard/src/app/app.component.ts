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

import { Component } from '@angular/core';
import { fromEvent, merge } from 'rxjs';
import { map, startWith } from 'rxjs/operators';

import { StatusService } from '@flink-runtime-web/services';

@Component({
  selector: 'flink-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.less']
})
export class AppComponent {
  collapsed = false;
  visible = false;
  online$ = merge(
    fromEvent(window, 'offline').pipe(map(() => false)),
    fromEvent(window, 'online').pipe(map(() => true))
  ).pipe(startWith(true));

  historyServerEnv = this.statusService.configuration.features['web-history'];
  webSubmitEnabled = this.statusService.configuration.features['web-submit'];

  showMessage(): void {
    if (this.statusService.listOfErrorMessage.length) {
      this.visible = true;
    }
  }

  clearMessage(): void {
    this.statusService.listOfErrorMessage = [];
    this.visible = false;
  }

  toggleCollapse(): void {
    this.collapsed = !this.collapsed;
  }

  constructor(public statusService: StatusService) {}
}
