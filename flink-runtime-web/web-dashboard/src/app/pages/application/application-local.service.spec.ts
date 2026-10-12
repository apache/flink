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

import { firstValueFrom } from 'rxjs';

import { ApplicationDetail } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it } from 'vitest';

import { ApplicationLocalService } from './application-local.service';

describe('ApplicationLocalService', () => {
  let service: ApplicationLocalService;

  beforeEach(() => {
    service = new ApplicationLocalService();
  });

  it('replays the latest application detail to a late subscriber', async () => {
    const application = { id: 'app-1' } as ApplicationDetail;
    service.setApplicationDetail(application);

    const result = await firstValueFrom(service.applicationDetailChanges());

    expect(result).toBe(application);
  });

  it('replays only the most recent application detail', async () => {
    service.setApplicationDetail({ id: 'app-1' } as ApplicationDetail);
    service.setApplicationDetail({ id: 'app-2' } as ApplicationDetail);

    const result = await firstValueFrom(service.applicationDetailChanges());

    expect(result.id).toBe('app-2');
  });
});
