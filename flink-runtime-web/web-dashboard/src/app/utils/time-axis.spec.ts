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

import { startAlignedTicks } from './time-axis';

describe('startAlignedTicks', () => {
  const DAY_MS = 86_400_000;

  it('anchors ticks at the domain minimum', () => {
    const min = new Date(1_000);
    const ticks = startAlignedTicks(min, new Date(1_000 + 60_000));
    expect(ticks[0]).toEqual(min);
    expect(ticks.length).toBeLessThanOrEqual(8);
  });

  it('uses sub-second steps for short ranges', () => {
    expect(startAlignedTicks(new Date(0), new Date(300)).map(Number)).toEqual([0, 50, 100, 150, 200, 250, 300]);
  });

  it('keeps the tick count bounded for ranges beyond the largest step', () => {
    const min = new Date(0);
    const ticks = startAlignedTicks(min, new Date(30 * DAY_MS));
    expect(ticks[0]).toEqual(min);
    expect(ticks.length).toBeLessThanOrEqual(8);
  });
});
