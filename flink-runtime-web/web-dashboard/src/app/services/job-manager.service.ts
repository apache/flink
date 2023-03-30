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

import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import {
  JobManagerLogItem,
  JobManagerThreadDump,
  JobMetric,
  MetricMap,
  JobManagerLogDetail,
  ClusterConfiguration,
  EnvironmentInfo
} from '@flink-runtime-web/interfaces';

import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})
export class JobManagerService {
  constructor(private readonly httpClient: HttpClient, private readonly configService: ConfigService) {}

  loadConfig(): Observable<ClusterConfiguration[]> {
    return this.httpClient.get<ClusterConfiguration[]>(`${this.configService.BASE_URL}/jobmanager/config`);
  }

  loadEnvironment(): Observable<EnvironmentInfo> {
    return this.httpClient.get<EnvironmentInfo>(`${this.configService.BASE_URL}/jobmanager/environment`);
  }

  loadLogs(): Observable<string> {
    return this.httpClient.get(`${this.configService.BASE_URL}/jobmanager/log`, {
      responseType: 'text',
      headers: new HttpHeaders().append('Cache-Control', 'no-cache')
    });
  }

  loadStdout(): Observable<string> {
    return this.httpClient.get(`${this.configService.BASE_URL}/jobmanager/stdout`, {
      responseType: 'text',
      headers: new HttpHeaders().append('Cache-Control', 'no-cache')
    });
  }

  loadLogList(): Observable<JobManagerLogItem[]> {
    return this.httpClient
      .get<{ logs: JobManagerLogItem[] }>(`${this.configService.BASE_URL}/jobmanager/logs`)
      .pipe(map(data => data.logs));
  }

  loadLog(logName: string): Observable<JobManagerLogDetail> {
    const url = `${this.configService.BASE_URL}/jobmanager/logs/${logName}`;
    return this.httpClient
      .get(url, { responseType: 'text', headers: new HttpHeaders().append('Cache-Control', 'no-cache') })
      .pipe(
        map(data => {
          return {
            data,
            url
          };
        })
      );
  }

  loadThreadDump(): Observable<string> {
    return this.httpClient.get<JobManagerThreadDump>(`${this.configService.BASE_URL}/jobmanager/thread-dump`).pipe(
      map(JobManagerThreadDump => {
        return JobManagerThreadDump.threadInfos.map(threadInfo => threadInfo.stringifiedThreadInfo).join('');
      })
    );
  }

  loadMetricsName(): Observable<string[]> {
    return this.httpClient
      .get<Array<{ id: string }>>(`${this.configService.BASE_URL}/jobmanager/metrics`)
      .pipe(map(arr => arr.map(item => item.id)));
  }

  loadMetrics(listOfMetricName: string[]): Observable<MetricMap> {
    const metricName = listOfMetricName.join(',');
    return this.httpClient
      .get<JobMetric[]>(`${this.configService.BASE_URL}/jobmanager/metrics`, { params: { get: metricName } })
      .pipe(
        map(arr => {
          const result: MetricMap = {};
          arr.forEach(item => {
            result[item.id] = parseInt(item.value, 10);
          });
          return result;
        })
      );
  }

  loadHistoryServerConfig(jobId: string): Observable<ClusterConfiguration[]> {
    return this.httpClient.get<ClusterConfiguration[]>(
      `${this.configService.BASE_URL}/jobs/${jobId}/jobmanager/config`
    );
  }

  loadHistoryServerEnvironment(jobId: string): Observable<EnvironmentInfo> {
    return this.httpClient.get<EnvironmentInfo>(`${this.configService.BASE_URL}/jobs/${jobId}/jobmanager/environment`);
  }

  loadHistoryServerJobManagerLogUrl(jobId: string): Observable<string> {
    return this.httpClient
      .get<{ url: string }>(`${this.configService.BASE_URL}/jobs/${jobId}/jobmanager/log-url`)
      .pipe(map(data => data.url));
  }
}
