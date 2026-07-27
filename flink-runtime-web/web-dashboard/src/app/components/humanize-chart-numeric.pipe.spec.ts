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

import { describe, expect, it } from 'vitest';

import { HumanizeChartNumericPipe } from './humanize-chart-numeric.pipe';

describe('HumanizeChartNumericPipe', () => {
  const pipe = new HumanizeChartNumericPipe();

  it('returns a dash for a nil value regardless of metric id', () => {
    expect(pipe.transform(null as unknown as number, 'numRecords')).toBe('-');
  });

  it('appends a per-second suffix to humanized bytes when the id matches both bytes and persecond', () => {
    expect(pipe.transform(2048, 'bytesPerSecond')).toBe('2.00 KB / s');
  });

  it('humanizes bytes without a rate suffix when the id only matches bytes', () => {
    expect(pipe.transform(2048, 'numBytesOut')).toBe('2.00 KB');
  });

  it('appends a per-second suffix to the raw value when the id only matches persecond', () => {
    expect(pipe.transform(500, 'numRecordsInPerSecond')).toBe('500 / s');
  });

  it('humanizes time/latency metrics as a short duration', () => {
    expect(pipe.transform(3665000, 'uptime')).toBe('1h 1m');
    expect(pipe.transform(3665000, 'latency')).toBe('1h 1m');
  });

  it('falls back to the raw value for unrecognized metric ids', () => {
    expect(pipe.transform(42, 'numRecords')).toBe('42');
  });
});
