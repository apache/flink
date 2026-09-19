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

import { HttpClient } from '@angular/common/http';
import { ChangeDetectorRef } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { EMPTY, firstValueFrom, of } from 'rxjs';

import { Configuration } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { StatusService } from './status.service';

const mockConfig = { 'refresh-interval': 1000, features: {}, 'flink-version': '2.4', 'flink-revision': 'abc' };

describe('StatusService', () => {
  let service: StatusService;
  const get = vi.fn();

  beforeEach(() => {
    get.mockReset().mockReturnValue(of(mockConfig));
    TestBed.configureTestingModule({
      providers: [
        StatusService,
        { provide: HttpClient, useValue: { get } },
        { provide: ConfigService, useValue: new ConfigService() },
        { provide: Router, useValue: { events: EMPTY } }
      ]
    });
    service = TestBed.inject(StatusService);
  });

  it('starts with no cached errors and a zero network-failure count', () => {
    expect(service.listOfErrorMessage).toEqual([]);
    expect(service.networkFailureCount).toBe(0);
    expect(service.networkFailureThreshold).toBe(5);
    expect(service.networkErrorNotificationId).toBeNull();
  });

  it('forwards markAppForCheck to the registered change detector', () => {
    const markForCheck = vi.fn();
    service.registerAppCdr({ markForCheck } as unknown as ChangeDetectorRef);

    service.markAppForCheck();

    expect(markForCheck).toHaveBeenCalledOnce();
  });

  it('is a no-op when markAppForCheck runs before any detector is registered', () => {
    expect(() => service.markAppForCheck()).not.toThrow();
  });

  it('caches the fetched configuration on boot', async () => {
    const result = await firstValueFrom(service.boot());

    expect(get).toHaveBeenCalledWith('./config');
    expect(result).toEqual(mockConfig);
    expect(service.configuration).toEqual(mockConfig as unknown as Configuration);
  });

  it('emits on the refresh stream once it is wired up by boot', async () => {
    vi.useFakeTimers();
    try {
      await firstValueFrom(service.boot());

      const emitted: boolean[] = [];
      const sub = service.refresh$.subscribe(value => emitted.push(value));
      // The merged trigger stream is debounced by 300ms before the interval kicks in.
      vi.advanceTimersByTime(300);
      sub.unsubscribe();

      expect(emitted).toContain(true);
    } finally {
      vi.useRealTimers();
    }
  });
});
