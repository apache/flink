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

import { HumanizeDatePipe } from './humanize-date.pipe';

describe('HumanizeDatePipe', () => {
  const pipe = new HumanizeDatePipe('en-US');

  it('renders an en dash for null, empty string, NaN, or negative values', () => {
    expect(pipe.transform(null as unknown as number)).toBe('–');
    expect(pipe.transform('')).toBe('–');
    expect(pipe.transform(NaN)).toBe('–');
    expect(pipe.transform(-1)).toBe('–');
    expect(pipe.transform('-1')).toBe('–');
  });

  it('formats an epoch timestamp using the given format and timezone', () => {
    expect(pipe.transform(0, 'yyyy-MM-dd', 'UTC')).toBe('1970-01-01');
  });

  it('formats a Date instance', () => {
    expect(pipe.transform(new Date(0), 'yyyy-MM-dd', 'UTC')).toBe('1970-01-01');
  });

  it('falls back to the default mediumDate format when none is provided', () => {
    expect(pipe.transform(0, undefined, 'UTC')).toBe('Jan 1, 1970');
  });

  it('swallows formatting errors and returns undefined when the format needs unloaded extra locale data', () => {
    // 'B' (flexible day period, e.g. "in the morning") requires extra Angular locale data
    // that plain "en-US" doesn't register by default, so formatDate throws internally.
    expect(pipe.transform(0, 'B', 'UTC')).toBeUndefined();
  });
});
