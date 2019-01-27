/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { NzMessageService } from 'ng-zorro-antd';
import { ConfigService } from './config.service';
import { Subject } from 'rxjs';
import { map, tap } from 'rxjs/operators';
import { TaskManagerListInterface, TaskManagerDetailInterface, JMXInterface } from 'flink-interfaces';

@Injectable({
  providedIn: 'root'
})
export class TaskManagerService {
  taskManagerDetail: TaskManagerDetailInterface;
  taskManagerDetail$ = new Subject<TaskManagerDetailInterface>();

  loadManagers() {
    return this.httpClient.get<TaskManagerListInterface>(
      `${this.configService.BASE_URL}/taskmanagers`).pipe(map(data => data.taskmanagers || [])
    );
  }

  loadManager(taskManagerId) {
    return this.httpClient.get<TaskManagerDetailInterface>(`${this.configService.BASE_URL}/taskmanagers/${taskManagerId}`);
  }

  /** @deprecated use loadLogList & loadLog instead **/
  loadLogs(taskManagerId) {
    return this.httpClient.get(`${this.configService.BASE_URL}/taskmanagers/${taskManagerId}/log`, { responseType: 'text' });
  }

  loadLogList(taskManagerId) {
    return this.httpClient.get<{ logs: Array<{ name: string, size: number }> }>(
      `${this.configService.BASE_URL}/taskmanagers/${taskManagerId}/logs`
    );
  }

  loadLog(taskManagerId, logName, page = 1, count = 102400) {
    const start = (page - 1) * count;
    const params = new HttpParams().append('start', `${start}`).append('count', `${count}`);
    return this.httpClient.get<{ data: string, file_size: number }>(
      `${this.configService.BASE_URL}/taskmanagers/${taskManagerId}/log/${logName}`, { params: params }
    );
  }

  loadStdout(taskManagerId) {
    return this.httpClient.get(`${this.configService.BASE_URL}/taskmanagers/${taskManagerId}/stdout`, { responseType: 'text' });
  }

  getJMX(taskManagerId) {
    return this.httpClient.get<JMXInterface>(`${this.configService.BASE_URL}/taskmanagers/${taskManagerId}/jmx`).pipe(tap(data => {
      if (data) {
        this.nzMessageService.info(`JMX Link: ${data.host}:${data.port}`, { nzDuration: 3000 });
      } else {
        this.nzMessageService.info(`JMX Link Not Found`, { nzDuration: 3000 });
      }
    }));
  }

  constructor(private httpClient: HttpClient, private configService: ConfigService, private nzMessageService: NzMessageService) {
  }
}
