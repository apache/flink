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
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { MetricMap, JobMetric, Watermarks, MetricMapWithTimestamp } from '@flink-runtime-web/interfaces';

import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})
export class MetricsService {
  constructor(private readonly httpClient: HttpClient, private readonly configService: ConfigService) {}

  public loadAllAvailableMetrics(jobId: string, vertexId: string): Observable<JobMetric[]> {
    return this.httpClient
      .get<JobMetric[]>(`${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/metrics`)
      .pipe(
        map(item =>
          item.sort((pre, next) => {
            const preId = pre.id.toLowerCase();
            const nextId = next.id.toLowerCase();
            if (preId < nextId) {
              return -1;
            } else if (preId > nextId) {
              return 1;
            } else {
              return 0;
            }
          })
        )
      );
  }

  public loadMetrics(jobId: string, vertexId: string, listOfMetricName: string[]): Observable<MetricMapWithTimestamp> {
    const metricName = listOfMetricName.join(',');
    return this.httpClient
      .get<JobMetric[]>(`${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/metrics`, {
        params: { get: metricName }
      })
      .pipe(
        map(arr => {
          const result: MetricMap = {};
          arr.forEach(item => {
            result[item.id] = parseInt(item.value, 10);
          });
          return {
            timestamp: Date.now(),
            values: result
          };
        })
      );
  }

  /** Get aggregated metric data from all subtasks of the given vertexId. */
  public loadAggregatedMetrics(
    jobId: string,
    vertexId: string,
    listOfMetricName: string[],
    aggregate: string = 'max'
  ): Observable<MetricMap> {
    const metricName = listOfMetricName.join(',');
    return this.httpClient
      .get<Array<{ id: string; min: number; max: number; avg: number; sum: number }>>(
        `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/subtasks/metrics`,
        { params: { get: metricName } }
      )
      .pipe(
        map(arr => {
          const result: MetricMap = {};
          arr.forEach(item => {
            switch (aggregate) {
              case 'min':
                result[item.id] = +item.min;
                break;
              case 'max':
                result[item.id] = +item.max;
                break;
              case 'avg':
                result[item.id] = +item.avg;
                break;
              case 'sum':
                result[item.id] = +item.sum;
                break;
              default:
                throw new Error(`Unsupported aggregate: ${aggregate}`);
            }
          });
          return result;
        })
      );
  }

  public loadWatermarks(jobId: string, vertexId: string): Observable<Watermarks> {
    return this.httpClient
      .get<JobMetric[]>(`${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/watermarks`)
      .pipe(
        map(arr => {
          let minValue = NaN;
          let lowWatermark: number;
          const watermarks: MetricMap = {};
          arr.forEach(item => {
            const value = parseInt(item.value, 10);
            const subTaskIndex = item.id.replace('.currentInputWatermark', '');
            watermarks[subTaskIndex] = value;
            if (isNaN(minValue) || value < minValue) {
              minValue = value;
            }
          });
          if (!isNaN(minValue) && minValue > this.configService.LONG_MIN_VALUE) {
            lowWatermark = minValue;
          } else {
            lowWatermark = NaN;
          }
          return {
            lowWatermark,
            watermarks
          };
        })
      );
  }
}
