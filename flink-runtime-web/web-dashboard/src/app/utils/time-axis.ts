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

import { timeFormat } from 'd3-time-format';

// g2 5's string labelFormatter uses d3's numeric formatter, so a time scale needs
// a function; mirror the previous 'HH:mm:ss.SSS' mask.
export const formatTimeAxis = timeFormat('%H:%M:%S.%L');

const TICK_STEPS_MS = [
  ...[1, 2, 5, 10, 20, 50, 100, 200, 500],
  ...[1, 2, 5, 10, 15, 30, 60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400].map(s => s * 1000)
];

const MAX_TICKS = 7;

// Unlike g2's default "nice" ticks, start at the domain minimum so the axis is anchored to the actual start time.
// Beyond the largest step, spread the ticks evenly instead so the axis never gets one tick per day.
export function startAlignedTicks(min: Date, max: Date): Date[] {
  const range = Number(max) - Number(min);
  const step = TICK_STEPS_MS.find(s => range / s <= MAX_TICKS) ?? Math.ceil(range / MAX_TICKS);
  const ticks: Date[] = [];
  for (let t = Number(min); t <= Number(max) && step > 0; t += step) {
    ticks.push(new Date(t));
  }
  return ticks;
}
