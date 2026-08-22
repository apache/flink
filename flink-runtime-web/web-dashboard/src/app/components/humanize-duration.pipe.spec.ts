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

import { HumanizeDurationPipe } from './humanize-duration.pipe';

describe('HumanizeDurationPipe', () => {
  const pipe = new HumanizeDurationPipe();

  it('returns a dash for nil, NaN, or negative durations', () => {
    expect(pipe.transform(null as unknown as number)).toBe('-');
    expect(pipe.transform(undefined as unknown as number)).toBe('-');
    expect(pipe.transform(NaN)).toBe('-');
    expect(pipe.transform(-1)).toBe('-');
  });

  it('renders sub-second durations as milliseconds only', () => {
    expect(pipe.transform(500)).toBe('500ms');
  });

  it('renders sub-minute durations as seconds and milliseconds', () => {
    expect(pipe.transform(5000)).toBe('5s 0ms');
  });

  it('renders sub-hour durations as minutes, seconds, and milliseconds', () => {
    expect(pipe.transform(65000)).toBe('1m 5s 0ms');
  });

  it('renders sub-day durations as hours, minutes, and seconds', () => {
    expect(pipe.transform(3665000)).toBe('1h 1m 5s');
  });

  it('drops seconds in short mode once hours are involved', () => {
    expect(pipe.transform(3665000, true)).toBe('1h 1m');
  });

  it('renders multi-day durations with full precision', () => {
    expect(pipe.transform(90000000)).toBe('1d 1h 0m 0s');
  });

  it('collapses to days and hours only in short mode', () => {
    expect(pipe.transform(90000000, true)).toBe('1d 1h');
  });
});
