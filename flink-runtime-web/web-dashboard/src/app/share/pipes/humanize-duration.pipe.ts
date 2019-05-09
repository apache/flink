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

import { Pipe, PipeTransform } from '@angular/core';
import { isNil } from 'utils';

@Pipe({
  name: 'humanizeDuration'
})
export class HumanizeDurationPipe implements PipeTransform {
  transform(value: number): any {
    if (isNil(value)) {
      return '-';
    } else if (value < 0) {
      return '-';
    } else {
      const ms = value % 1000;
      let x = Math.floor(value / 1000);
      const seconds = x % 60;
      x = Math.floor(x / 60);
      const minutes = x % 60;
      x = Math.floor(x / 60);
      const hours = x % 24;
      x = Math.floor(x / 24);
      const days = x;
      if (days === 0) {
        if (hours === 0) {
          if (minutes === 0) {
            if (seconds === 0) {
              return `${ms}ms`;
            } else {
              return `${seconds}s`;
            }
          } else {
            return `${minutes}m ${seconds}s`;
          }
        } else {
          return `${hours}h ${minutes}m`;
        }
      } else {
        return `${days}d ${hours}h ${minutes}m`;
      }
    }
  }
}
