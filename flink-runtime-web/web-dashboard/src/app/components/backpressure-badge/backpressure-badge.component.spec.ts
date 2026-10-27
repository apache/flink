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

import { BackpressureBadgeComponent } from './backpressure-badge.component';

describe('BackpressureBadgeComponent', () => {
  it('maps each known backpressure state to its color', () => {
    const component = new BackpressureBadgeComponent();

    component.state = 'ok';
    expect(component.backgroundColor).toBe('#52c41a');

    component.state = 'low';
    expect(component.backgroundColor).toBe('#faad14');

    component.state = 'high';
    expect(component.backgroundColor).toBe('#f5222d');

    component.state = 'in-progress';
    expect(component.backgroundColor).toBe('#f5222d');
  });

  it('is case-insensitive', () => {
    const component = new BackpressureBadgeComponent();

    component.state = 'OK';

    expect(component.backgroundColor).toBe('#52c41a');
  });

  it('returns undefined for an unrecognized or missing state', () => {
    const component = new BackpressureBadgeComponent();

    component.state = 'unknown';
    expect(component.backgroundColor).toBeUndefined();

    component.state = undefined as unknown as string;
    expect(component.backgroundColor).toBeUndefined();
  });
});
