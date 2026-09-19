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

import { ParseIntPipe } from './parse-int.pipe';

describe('ParseIntPipe', () => {
  const pipe = new ParseIntPipe();

  it('parses a numeric string to an integer', () => {
    expect(pipe.transform('42')).toBe(42);
  });

  it('truncates a decimal string down to its integer part', () => {
    expect(pipe.transform('42.9')).toBe(42);
  });

  it('parses the leading numeric portion of a mixed string', () => {
    expect(pipe.transform('12px')).toBe(12);
  });

  it('returns null for a non-numeric string', () => {
    expect(pipe.transform('not-a-number')).toBeNull();
  });

  it('returns null for an empty string', () => {
    expect(pipe.transform('')).toBeNull();
  });
});
