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

import { ConfigService } from '@flink-runtime-web/services';

@Pipe({
  name: 'humanizeWatermark',
  standalone: true
})
export class HumanizeWatermarkPipe implements PipeTransform {
  constructor(private readonly configService: ConfigService) {}

  public transform(value: number): number | string {
    if (isNaN(value) || value <= this.configService.LONG_MIN_VALUE) {
      return 'No Watermark (Watermarks are only available if EventTime is used)';
    } else {
      return value;
    }
  }
}

@Pipe({
  name: 'humanizeWatermarkToDatetime',
  standalone: true
})
export class HumanizeWatermarkToDatetimePipe implements PipeTransform {
  constructor(private readonly configService: ConfigService) {}

  public transform(value: number, timezone: string = 'UTC'): number | string {
    if (value == null || isNaN(value) || value <= this.configService.LONG_MIN_VALUE) {
      return 'N/A';
    }

    try {
      const date = new Date(value);

      // Use Intl.DateTimeFormat for proper timezone handling including DST
      // This native browser API automatically handles daylight saving time transitions
      // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/DateTimeFormat
      const dateFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        hour12: false,
        minute: '2-digit',
        second: '2-digit'
      });

      // Get timezone abbreviation (e.g., PST, PDT, EST, EDT)
      // The abbreviation automatically reflects DST status (e.g., PST vs PDT)
      const timezoneFormatter = new Intl.DateTimeFormat('en-US', {
        timeZone: timezone,
        timeZoneName: 'short'
      });

      // Format the date parts
      const parts = dateFormatter.formatToParts(date);
      const year = parts.find(p => p.type === 'year')?.value;
      const month = parts.find(p => p.type === 'month')?.value;
      const day = parts.find(p => p.type === 'day')?.value;
      const hour = parts.find(p => p.type === 'hour')?.value;
      const minute = parts.find(p => p.type === 'minute')?.value;
      const second = parts.find(p => p.type === 'second')?.value;

      // Extract timezone abbreviation which includes DST information
      // For example: PST (standard) vs PDT (daylight saving)
      const timezoneParts = timezoneFormatter.formatToParts(date);
      const timezoneAbbr = timezoneParts.find(p => p.type === 'timeZoneName')?.value || timezone;

      return `${year}-${month}-${day} ${hour}:${minute}:${second} (${timezoneAbbr})`;
    } catch (error) {
      // Fallback to UTC if timezone is invalid, so using UTC
      console.error('[HumanizeWatermarkToDatetimePipe] Error formatting date, falling back to UTC:', error);
      const date = new Date(value);
      const year = date.getUTCFullYear();
      const month = String(date.getUTCMonth() + 1).padStart(2, '0');
      const day = String(date.getUTCDate()).padStart(2, '0');
      const hour = String(date.getUTCHours()).padStart(2, '0');
      const minute = String(date.getUTCMinutes()).padStart(2, '0');
      const second = String(date.getUTCSeconds()).padStart(2, '0');
      return `${year}-${month}-${day} ${hour}:${minute}:${second} (UTC)`;
    }
  }
}
