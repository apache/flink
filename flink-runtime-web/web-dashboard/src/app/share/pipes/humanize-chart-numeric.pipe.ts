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
import { HumanizeBytesPipe } from './humanize-bytes.pipe';
import { HumanizeDurationPipe } from './humanize-duration.pipe';

@Pipe({
  name: 'humanizeChartNumeric'
})
export class HumanizeChartNumericPipe implements PipeTransform {
  transform(value: number, id: string): string {
    if (isNil(value)) {
      return '-';
    }
    let returnVal = '';
    if (/bytes/i.test(id) && /persecond/i.test(id)) {
      returnVal = new HumanizeBytesPipe().transform(value) + ' / s';
    } else if (/bytes/i.test(id)) {
      returnVal = new HumanizeBytesPipe().transform(value);
    } else if (/persecond/i.test(id)) {
      returnVal = value + ' / s';
    } else if (/time/i.test(id) || /latency/i.test(id)) {
      returnVal = new HumanizeDurationPipe().transform(value, true);
    } else {
      returnVal = `${value}`;
    }
    return returnVal;
  }
}
