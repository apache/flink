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

import { ColorKey, ConfigService } from './config.service';

// Every status the UI can render a badge/timeline bar for. A key missing from COLOR_MAP would
// render blank instead of coloured, so this list guards against a status being added without one.
const COLOR_KEYS: ColorKey[] = [
  'TOTAL',
  'RUNNING',
  'FAILED',
  'FINISHED',
  'CANCELED',
  'CANCELING',
  'CREATED',
  'DEPLOYING',
  'RECONCILING',
  'IN_PROGRESS',
  'SCHEDULED',
  'COMPLETED',
  'RESTARTING',
  'PENDING',
  'INITIALIZING',
  'IGNORED'
];

describe('ConfigService', () => {
  const service = new ConfigService();

  it('defaults the REST base url to the current origin', () => {
    expect(service.BASE_URL).toBe('.');
  });

  it('maps every known status key to a hex colour with no extras', () => {
    expect(Object.keys(service.COLOR_MAP).sort()).toEqual([...COLOR_KEYS].sort());
    for (const key of COLOR_KEYS) {
      expect(service.COLOR_MAP[key]).toMatch(/^#[0-9a-f]{6}$/i);
    }
  });
});
