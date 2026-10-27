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

import { HumanizeBytesPipe } from './humanize-bytes.pipe';

describe('HumanizeBytesPipe', () => {
  const pipe = new HumanizeBytesPipe();

  it('returns a dash for nil, NaN, or negative values', () => {
    expect(pipe.transform(null as unknown as number)).toBe('-');
    expect(pipe.transform(undefined as unknown as number)).toBe('-');
    expect(pipe.transform(NaN)).toBe('-');
    expect(pipe.transform(-1)).toBe('-');
  });

  it('renders sub-1000 values as whole bytes', () => {
    expect(pipe.transform(0)).toBe('0 B');
    expect(pipe.transform(999)).toBe('999 B');
  });

  it('renders values below one KB unit with two decimal places', () => {
    expect(pipe.transform(1000)).toBe('0.98 KB');
    expect(pipe.transform(1023)).toBe('1.00 KB');
  });

  it('renders values within a tier with three significant digits', () => {
    expect(pipe.transform(1536)).toBe('1.50 KB');
    // Just below the KB->MB rollover boundary (1024 * 1000): still three-sig-fig KB, rounds up to 1.00e+3.
    expect(pipe.transform(1023999)).toBe('1.00e+3 KB');
    expect(pipe.transform(999 * 1024)).toBe('999 KB');
  });

  it('rolls over into the next unit once a tier is exceeded', () => {
    // Exactly at the KB->MB rollover boundary (1024 * 1000): recurses into MB and lands back in the toFixed(2) branch.
    expect(pipe.transform(1024 * 1000)).toBe('0.98 MB');
    expect(pipe.transform(1024 * 1024)).toBe('1.00 MB');
  });
});
