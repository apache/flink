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
import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})
export class JobManagerService {

  loadConfig() {
    return this.httpClient.get<Array<{ key: string; value: string; }>>(`${this.configService.BASE_URL}/jobmanager/config`);
  }

  loadLogs(page = -1, count = 10240) {
    const start = page * count;
    const params = new HttpParams().append('start', `${start}`).append('count', `${count}`);
    return this.httpClient.get(`${this.configService.BASE_URL}/jobmanager/log`, { params: params, responseType: 'text' });
  }

  loadStdout(page = -1, count = 10240) {
    const start = page * count;
    const params = new HttpParams().append('start', `${start}`).append('count', `${count}`);
    return this.httpClient.get(`${this.configService.BASE_URL}/jobmanager/stdout`, { params: params, responseType: 'text' });
  }

  constructor(private httpClient: HttpClient, private configService: ConfigService) {
  }
}
