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

import { AsyncPipe, NgForOf, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { fromEvent, merge } from 'rxjs';
import { map, startWith } from 'rxjs/operators';

import { StatusService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzLayoutModule } from 'ng-zorro-antd/layout';
import { NzMenuModule } from 'ng-zorro-antd/menu';

@Component({
  selector: 'flink-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    RouterLink,
    RouterLinkActive,
    RouterOutlet,
    AsyncPipe,
    NzLayoutModule,
    NzMenuModule,
    NzIconModule,
    NzDividerModule,
    NzBadgeModule,
    NzDrawerModule,
    NzAlertModule,
    NgIf,
    NgForOf
  ],
  standalone: true
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
      this.cdr.markForCheck();
    }
  }

  clearMessage(): void {
    this.statusService.listOfErrorMessage = [];
    this.visible = false;
    this.cdr.markForCheck();
  }

  toggleCollapse(): void {
    this.collapsed = !this.collapsed;
    this.cdr.markForCheck();
  }

  constructor(public statusService: StatusService, private cdr: ChangeDetectorRef) {}
}
