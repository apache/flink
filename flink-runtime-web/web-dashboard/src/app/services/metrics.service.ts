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
import { map } from 'rxjs/operators';
import { BASE_URL, LONG_MIN_VALUE } from 'config';

@Injectable({
  providedIn: 'root'
})
export class MetricsService {
  constructor(private httpClient: HttpClient) {}

  /**
   * Get available metric list
   * @param jobId
   * @param vertexId
   */
  getAllAvailableMetrics(jobId: string, vertexId: string) {
    return this.httpClient.get<Array<{ id: string; value: string }>>(
      `${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/metrics`
    );
  }

  /**
   * Get metric data
   * @param jobId
   * @param vertexId
   * @param listOfMetricName
   */
  getMetrics(jobId: string, vertexId: string, listOfMetricName: string[]) {
    const metricName = listOfMetricName.join(',');
    return this.httpClient
      .get<Array<{ id: string; value: string }>>(
        `${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/metrics?get=${metricName}`
      )
      .pipe(
        map(arr => {
          const result: { [id: string]: number } = {};
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

  /**
   * Get watermarks data
   * @param jobId
   * @param vertexId
   * @param parallelism
   */
  getWatermarks(jobId: string, vertexId: string, parallelism: number) {
    const listOfMetricName = new Array(parallelism).fill(0).map((_, index) => `${index}.currentInputWatermark`);
    return this.getMetrics(jobId, vertexId, listOfMetricName).pipe(
      map(metrics => {
        let minValue = NaN;
        let lowWatermark = NaN;
        const watermarks: { [id: string]: number } = {};
        const ref = metrics.values;
        for (const key in ref) {
          const value = ref[key];
          const subTaskIndex = key.replace('.currentInputWatermark', '');
          watermarks[subTaskIndex] = value;
          if (isNaN(minValue) || value < minValue) {
            minValue = value;
          }
        }
        if (!isNaN(minValue) && minValue > LONG_MIN_VALUE) {
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
