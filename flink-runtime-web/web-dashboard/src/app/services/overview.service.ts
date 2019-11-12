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
import { EMPTY } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { BASE_URL } from '../app.config';
import { OverviewInterface } from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class OverviewService {
  constructor(private httpClient: HttpClient) {}

  /**
   * Get cluster overview status
   */
  loadOverview() {
    return this.httpClient.get<OverviewInterface>(`${BASE_URL}/overview`).pipe(catchError(() => EMPTY));
  }
}
