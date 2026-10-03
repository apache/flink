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

import { ApplicationBadgeComponent } from './application-badge.component';

describe('ApplicationBadgeComponent', () => {
  const configService = new ConfigService();
  const component = new ApplicationBadgeComponent(configService);

  it('resolves the color configured for a known status', () => {
    expect(component.backgroundColor('RUNNING')).toBe(configService.COLOR_MAP.RUNNING);
    expect(component.backgroundColor('FAILED')).toBe(configService.COLOR_MAP.FAILED);
  });

  it('returns undefined for an unrecognized status', () => {
    expect(component.backgroundColor('NOT_A_REAL_STATUS')).toBeUndefined();
  });
});
