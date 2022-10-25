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

import { formatDate } from '@angular/common';
import { Inject, LOCALE_ID, Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'humanizeDate',
  standalone: true
})
export class HumanizeDatePipe implements PipeTransform {
  constructor(@Inject(LOCALE_ID) private readonly locale: string) {}

  public transform(
    value: number | string | Date,
    format = 'mediumDate',
    timezone?: string,
    locale?: string
  ): string | null | undefined {
    if (value == null || value === '' || value !== value || value < 0) {
      return '–';
    }

    try {
      return formatDate(value, format, locale || this.locale, timezone);
    } catch (error) {}
  }
}
