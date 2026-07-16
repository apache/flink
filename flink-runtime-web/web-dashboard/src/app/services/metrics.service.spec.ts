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
import { firstValueFrom, of } from 'rxjs';

import { JobMetric } from '@flink-runtime-web/interfaces';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ConfigService } from './config.service';
import { MetricsService } from './metrics.service';

describe('MetricsService', () => {
  let httpClient: { get: ReturnType<typeof vi.fn> };
  let configService: ConfigService;
  let service: MetricsService;

  beforeEach(() => {
    httpClient = { get: vi.fn() };
    configService = new ConfigService();
    service = new MetricsService(httpClient as unknown as HttpClient, configService);
  });

  describe('loadAllAvailableMetrics', () => {
    it('sorts the available metrics by id, case-insensitively', async () => {
      const metrics: JobMetric[] = [
        { id: 'Zebra', value: '1' },
        { id: 'apple', value: '2' },
        { id: 'Banana', value: '3' }
      ];
      httpClient.get.mockReturnValue(of(metrics));

      const result = await firstValueFrom(service.loadAllAvailableMetrics('job-1', 'vertex-1'));

      expect(result.map(item => item.id)).toEqual(['apple', 'Banana', 'Zebra']);
    });
  });

  describe('loadMetrics', () => {
    it('parses each metric value to a float and stamps the result with a timestamp', async () => {
      httpClient.get.mockReturnValue(
        of([
          { id: 'numRecordsIn', value: '12.5' },
          { id: 'numRecordsOut', value: '7' }
        ])
      );

      const before = Date.now();
      const result = await firstValueFrom(service.loadMetrics('job-1', 'vertex-1', ['numRecordsIn', 'numRecordsOut']));

      expect(result.values).toEqual({ numRecordsIn: 12.5, numRecordsOut: 7 });
      expect(result.timestamp).toBeGreaterThanOrEqual(before);
      expect(httpClient.get).toHaveBeenCalledWith(expect.any(String), {
        params: { get: 'numRecordsIn,numRecordsOut' }
      });
    });
  });

  describe('loadMetricsWithAllAggregates', () => {
    it('coerces every aggregate field to a number', async () => {
      httpClient.get.mockReturnValue(
        of([{ id: 'numRecordsIn', min: '0', max: '10', avg: '5', sum: '15', skew: '66' }])
      );

      const result = await firstValueFrom(service.loadMetricsWithAllAggregates('job-1', 'vertex-1', ['numRecordsIn']));

      expect(result.numRecordsIn).toEqual({ min: 0, max: 10, avg: 5, sum: 15, skew: 66 });
    });
  });

  describe('loadAggregatedMetrics', () => {
    beforeEach(() => {
      vi.spyOn(service, 'loadMetricsWithAllAggregates').mockReturnValue(
        of({ numRecordsIn: { min: 1, max: 9, avg: 5, sum: 20, skew: 66 } })
      );
    });

    it.each([
      ['min', 1],
      ['max', 9],
      ['avg', 5],
      ['sum', 20],
      ['skew', 66]
    ])('extracts the %s aggregate', async (aggregate, expected) => {
      const result = await firstValueFrom(
        service.loadAggregatedMetrics('job-1', 'vertex-1', ['numRecordsIn'], aggregate)
      );

      expect(result.numRecordsIn).toBe(expected);
    });

    it('defaults to the max aggregate when none is given', async () => {
      const result = await firstValueFrom(service.loadAggregatedMetrics('job-1', 'vertex-1', ['numRecordsIn']));

      expect(result.numRecordsIn).toBe(9);
    });

    it('errors out for an unsupported aggregate type', async () => {
      await expect(
        firstValueFrom(service.loadAggregatedMetrics('job-1', 'vertex-1', ['numRecordsIn'], 'median'))
      ).rejects.toThrow('Unsupported aggregate: median');
    });
  });

  describe('loadWatermarks', () => {
    it('reports the lowest watermark across subtasks, keyed by subtask index', async () => {
      httpClient.get.mockReturnValue(
        of([
          { id: '0.currentInputWatermark', value: '100' },
          { id: '1.currentInputWatermark', value: '50' }
        ])
      );

      const result = await firstValueFrom(service.loadWatermarks('job-1', 'vertex-1'));

      expect(result.watermarks).toEqual({ '0': 100, '1': 50 });
      expect(result.lowWatermark).toBe(50);
    });

    it('reports NaN when every subtask is still at the Long.MIN_VALUE sentinel', async () => {
      httpClient.get.mockReturnValue(
        of([{ id: '0.currentInputWatermark', value: String(configService.LONG_MIN_VALUE) }])
      );

      const result = await firstValueFrom(service.loadWatermarks('job-1', 'vertex-1'));

      expect(result.lowWatermark).toBeNaN();
    });

    it('reports NaN when there are no subtasks', async () => {
      httpClient.get.mockReturnValue(of([]));

      const result = await firstValueFrom(service.loadWatermarks('job-1', 'vertex-1'));

      expect(result.lowWatermark).toBeNaN();
      expect(result.watermarks).toEqual({});
    });
  });
});
