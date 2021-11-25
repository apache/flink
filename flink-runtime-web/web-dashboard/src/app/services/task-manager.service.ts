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
import { EMPTY, Observable, of, ReplaySubject } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { BASE_URL } from 'config';
import {
  TaskManagerDetail,
  TaskManagerList,
  TaskManagerLogItem,
  TaskmanagersItem,
  TaskManagerThreadDump
} from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class TaskManagerService {
  public readonly taskManagerDetail$ = new ReplaySubject<TaskManagerDetail>(1);

  constructor(private readonly httpClient: HttpClient) {}

  public loadManagers(): Observable<TaskmanagersItem[]> {
    return this.httpClient.get<TaskManagerList>(`${BASE_URL}/taskmanagers`).pipe(
      map(data => data.taskmanagers || []),
      catchError(() => of([]))
    );
  }

  public loadManager(taskManagerId: string): Observable<TaskManagerDetail> {
    return this.httpClient
      .get<TaskManagerDetail>(`${BASE_URL}/taskmanagers/${taskManagerId}`)
      .pipe(catchError(() => EMPTY));
  }

  public loadLogList(taskManagerId: string): Observable<TaskManagerLogItem[]> {
    return this.httpClient
      .get<{ logs: TaskManagerLogItem[] }>(`${BASE_URL}/taskmanagers/${taskManagerId}/logs`)
      .pipe(map(data => data.logs));
  }

  public loadLog(taskManagerId: string, logName: string): Observable<{ data: string; url: string }> {
    const url = `${BASE_URL}/taskmanagers/${taskManagerId}/logs/${logName}`;
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

  public loadThreadDump(taskManagerId: string): Observable<string> {
    return this.httpClient.get<TaskManagerThreadDump>(`${BASE_URL}/taskmanagers/${taskManagerId}/thread-dump`).pipe(
      map(taskManagerThreadDump => {
        return taskManagerThreadDump.threadInfos.map(threadInfo => threadInfo.stringifiedThreadInfo).join('');
      })
    );
  }

  public loadLogs(taskManagerId: string): Observable<string> {
    return this.httpClient.get(`${BASE_URL}/taskmanagers/${taskManagerId}/log`, {
      responseType: 'text',
      headers: new HttpHeaders().append('Cache-Control', 'no-cache')
    });
  }

  public loadStdout(taskManagerId: string): Observable<string> {
    return this.httpClient.get(`${BASE_URL}/taskmanagers/${taskManagerId}/stdout`, {
      responseType: 'text',
      headers: new HttpHeaders().append('Cache-Control', 'no-cache')
    });
  }

  public getMetrics(taskManagerId: string, listOfMetricName: string[]): Observable<{ [p: string]: number }> {
    const metricName = listOfMetricName.join(',');
    return this.httpClient
      .get<Array<{ id: string; value: string }>>(`${BASE_URL}/taskmanagers/${taskManagerId}/metrics?get=${metricName}`)
      .pipe(
        map(arr => {
          const result: { [id: string]: number } = {};
          arr.forEach(item => {
            result[item.id] = parseInt(item.value, 10);
          });
          return result;
        })
      );
  }
}
