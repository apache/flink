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

import { HttpClient, HttpRequest, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ConfigService } from './config.service';
import { JarListInterface, PlanInterface } from 'flink-interfaces';

@Injectable({
  providedIn: 'root'
})
export class JarService {

  loadJarList() {
    return this.httpClient.get<JarListInterface>(`${this.configService.BASE_URL}/jars`);
  }

  uploadJar(fd) {
    const formData = new FormData();
    formData.append('jarfile', fd, fd.name);
    console.log(formData);
    const req = new HttpRequest('POST', `${this.configService.BASE_URL}/jars/upload`, formData, {
      reportProgress: true
    });
    return this.httpClient.request(req);
  }

  deleteJar(jarId) {
    return this.httpClient.delete(`${this.configService.BASE_URL}/jars/${jarId}`);
  }

  runJob(jarId, entryClass, parallelism, programArgs, savepointPath, allowNonRestoredState) {
    const requestParam = { entryClass, parallelism, programArgs, savepointPath, allowNonRestoredState };
    let params = new HttpParams();
    if (entryClass) {
      params = params.append('entry-class', entryClass);
    }
    if (parallelism) {
      params = params.append('parallelism', parallelism);
    }
    if (programArgs) {
      params = params.append('program-args', programArgs);
    }
    if (savepointPath) {
      params = params.append('savepointPath', programArgs);
    }
    if (allowNonRestoredState) {
      params = params.append('allowNonRestoredState', allowNonRestoredState);
    }
    return this.httpClient.post<{ jobid: string }>(`${this.configService.BASE_URL}/jars/${jarId}/run`, requestParam, { params });
  }

  getPlan(jarId, entryClass, parallelism, programArgs) {
    let params = new HttpParams();
    if (entryClass) {
      params = params.append('entry-class', entryClass);
    }
    if (parallelism) {
      params = params.append('parallelism', parallelism);
    }
    if (programArgs) {
      params = params.append('program-args', programArgs);
    }
    return this.httpClient.get<PlanInterface>(`${this.configService.BASE_URL}/jars/${jarId}/plan`, {
      params: params
    });
  }

  constructor(private httpClient: HttpClient, private configService: ConfigService) {
  }
}
