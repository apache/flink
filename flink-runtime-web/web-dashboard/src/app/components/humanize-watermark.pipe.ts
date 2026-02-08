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

  public transform(value: number, timezone: string = 'auto'): number | string {
    console.log('[HumanizeWatermarkToDatetimePipe] Transform called with value:', value, 'timezone:', timezone);

    if (value == null || isNaN(value) || value <= this.configService.LONG_MIN_VALUE) {
      console.log('[HumanizeWatermarkToDatetimePipe] Invalid value, returning N/A');
      return 'N/A';
    } else {
      let timezoneOffsetMinutes: number;

      // Parse timezone format, e.g. UTC+8, UTC-5, UTC+5:30, etc.
      const match = timezone.match(/UTC([+-])(\d+)(?::(\d+))?/);
      if (match) {
        const sign = match[1] === '+' ? 1 : -1;
        const hours = parseInt(match[2]);
        const minutes = match[3] ? parseInt(match[3]) : 0;
        timezoneOffsetMinutes = sign * (hours * 60 + minutes);
        console.log('[HumanizeWatermarkToDatetimePipe] Parsed timezone offset:', timezoneOffsetMinutes, 'minutes');
      } else {
        // Default to browser timezone if format is not recognized
        const browserOffset = new Date().getTimezoneOffset();
        timezoneOffsetMinutes = -browserOffset; // Convert to UTC offset format
        console.log(
          '[HumanizeWatermarkToDatetimePipe] Using browser timezone offset:',
          timezoneOffsetMinutes,
          'minutes'
        );
      }

      // Calculate the datetime in the target timezone
      // value is timestamp in milliseconds (UTC)
      const utcDate = new Date(value);
      const targetDate = new Date(utcDate.getTime() + timezoneOffsetMinutes * 60 * 1000);

      const year = targetDate.getUTCFullYear();
      const month = (targetDate.getUTCMonth() + 1).toString().padStart(2, '0');
      const day = targetDate.getUTCDate().toString().padStart(2, '0');
      const hours = targetDate.getUTCHours().toString().padStart(2, '0');
      const minutes = targetDate.getUTCMinutes().toString().padStart(2, '0');
      const seconds = targetDate.getUTCSeconds().toString().padStart(2, '0');

      // Format timezone string
      const absOffsetMinutes = Math.abs(timezoneOffsetMinutes);
      const timezoneHours = Math.floor(absOffsetMinutes / 60);
      const timezoneMinutes = absOffsetMinutes % 60;
      const timezoneSign = timezoneOffsetMinutes >= 0 ? '+' : '-';
      const timezoneString = `UTC${timezoneSign}${timezoneHours}${
        timezoneMinutes > 0 ? `:${timezoneMinutes.toString().padStart(2, '0')}` : ''
      }`;

      const result = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}(${timezoneString})`;
      console.log('[HumanizeWatermarkToDatetimePipe] Result:', result);
      return result;
    }
  }
}
