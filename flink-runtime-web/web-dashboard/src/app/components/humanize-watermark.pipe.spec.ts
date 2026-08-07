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

import { ConfigService } from '@flink-runtime-web/services';
import { describe, expect, it } from 'vitest';

import { HumanizeWatermarkPipe, HumanizeWatermarkToDatetimePipe } from './humanize-watermark.pipe';

describe('HumanizeWatermarkPipe', () => {
  const configService = new ConfigService();
  const pipe = new HumanizeWatermarkPipe(configService);

  it('passes a real watermark value through untouched', () => {
    expect(pipe.transform(1_700_000_000_000)).toBe(1_700_000_000_000);
  });

  it('reports no watermark for the Long.MIN_VALUE sentinel used when EventTime is unset', () => {
    expect(pipe.transform(configService.LONG_MIN_VALUE)).toBe(
      'No Watermark (Watermarks are only available if EventTime is used)'
    );
  });

  it('reports no watermark for values below the sentinel or NaN', () => {
    expect(pipe.transform(configService.LONG_MIN_VALUE - 1)).toBe(
      'No Watermark (Watermarks are only available if EventTime is used)'
    );
    expect(pipe.transform(NaN)).toBe('No Watermark (Watermarks are only available if EventTime is used)');
  });
});

describe('HumanizeWatermarkToDatetimePipe', () => {
  const configService = new ConfigService();
  const pipe = new HumanizeWatermarkToDatetimePipe(configService);

  it('returns N/A for nil, NaN, or sentinel values', () => {
    expect(pipe.transform(null as unknown as number)).toBe('N/A');
    expect(pipe.transform(NaN)).toBe('N/A');
    expect(pipe.transform(configService.LONG_MIN_VALUE)).toBe('N/A');
  });

  it('formats an epoch timestamp in the requested timezone with its abbreviation', () => {
    expect(pipe.transform(0, 'UTC')).toBe('1970-01-01 00:00:00 (UTC)');
  });

  it('defaults to UTC when no timezone is given', () => {
    expect(pipe.transform(0)).toBe('1970-01-01 00:00:00 (UTC)');
  });

  it('falls back to a manually formatted UTC string when the timezone is invalid', () => {
    expect(pipe.transform(0, 'not-a-real-timezone')).toBe('1970-01-01 00:00:00 (UTC)');
  });
});
