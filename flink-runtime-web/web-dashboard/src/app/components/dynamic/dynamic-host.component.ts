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

import {
  Component,
  ChangeDetectionStrategy,
  ViewChild,
  Input,
  Type,
  OnChanges,
  SimpleChanges,
  ComponentRef,
  ChangeDetectorRef
} from '@angular/core';

import { DynamicDirective } from '@flink-runtime-web/components/dynamic.directive';

@Component({
  selector: 'flink-dynamic-host',
  templateUrl: './dynamic-host.component.html',
  styleUrls: ['./dynamic-host.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [DynamicDirective],
  standalone: true
})
export class DynamicHostComponent implements OnChanges {
  @Input() data: Record<string, unknown> = {};
  /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
  @Input() component: Type<any>;
  /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
  componentRef?: ComponentRef<any>;
  @ViewChild(DynamicDirective, { static: true }) host!: DynamicDirective;
  constructor() {}

  ngOnChanges(simpleChanges: SimpleChanges): void {
    if (simpleChanges.component) {
      const viewContainerRef = this.host.viewContainerRef;
      viewContainerRef.clear();
      this.componentRef = viewContainerRef.createComponent(this.component);
    }
    if (simpleChanges.data) {
      if (this.componentRef) {
        const instance = this.componentRef.instance;
        Object.keys(this.data).forEach(k => {
          instance[k] = this.data[k];
        });
        // https://github.com/angular/angular/issues/36667
        // OnPush dynamic component never refresh by ComponentRef.changeDetectorRef
        this.componentRef!.injector.get(ChangeDetectorRef).markForCheck();
      }
    }
  }
}
