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
import { BASE_URL } from 'config';

@Injectable({
  providedIn: 'root'
})
export class JobManagerService {
  /**
   * Load JM config
   */
  loadConfig() {
    return this.httpClient.get<Array<{ key: string; value: string }>>(`${BASE_URL}/jobmanager/config`);
  }

  /**
   * Load JM logs
   */
  loadLogs() {
    return this.httpClient.get(`${BASE_URL}/jobmanager/log`, {
      responseType: 'text',
      headers: new HttpHeaders().append('Cache-Control', 'no-cache')
    });
  }

  /**
   * Load JM stdout
   */
  loadStdout() {
    return this.httpClient.get(`${BASE_URL}/jobmanager/stdout`, {
      responseType: 'text',
      headers: new HttpHeaders().append('Cache-Control', 'no-cache')
    });
  }

  constructor(private httpClient: HttpClient) {}
}
