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

import { HttpClient, HttpContext } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { EMPTY, Observable } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import {
  ApplicationDetail,
  ApplicationOverview,
  ApplicationItem,
  JobStatus,
  TaskStatus
} from '@flink-runtime-web/interfaces';
import { ApplicationExceptions } from '@flink-runtime-web/interfaces/application-exception';

import { ConfigService } from './config.service';
import { EXPECTED_NOT_FOUND, rethrowOnlyNotFound } from './http-context';

@Injectable({
  providedIn: 'root'
})
export class ApplicationService {
  constructor(
    private readonly httpClient: HttpClient,
    private readonly configService: ConfigService
  ) {}

  public cancelApplication(applicationId: string): Observable<void> {
    return this.httpClient.post<void>(`${this.configService.BASE_URL}/applications/${applicationId}/cancel`, {});
  }

  public loadApplications(): Observable<ApplicationItem[]> {
    return this.httpClient.get<ApplicationOverview>(`${this.configService.BASE_URL}/applications/overview`).pipe(
      map(data => {
        data.applications.forEach(application => {
          let total = 0;
          for (const key in application.jobs) {
            total += application.jobs[key as keyof JobStatus];
          }
          application.jobs['TOTAL'] = total;
          application.completed = ['FINISHED', 'FAILED', 'CANCELED'].indexOf(application.status) > -1;
        });
        return data.applications || [];
      }),
      catchError(() => EMPTY)
    );
  }

  public loadApplication(applicationId: string): Observable<ApplicationDetail> {
    const url = `${this.configService.BASE_URL}/applications/${applicationId}`;
    // A 404 means the application is gone (e.g. expired); callers handle it, so it is neither
    // notified as a server error nor swallowed like other errors.
    const context = new HttpContext().set(EXPECTED_NOT_FOUND, true);
    return this.httpClient.get<ApplicationDetail>(url, { context }).pipe(
      map(data => {
        const statusCounts: JobStatus = {
          CANCELED: 0,
          CANCELING: 0,
          CREATED: 0,
          FAILED: 0,
          FAILING: 0,
          FINISHED: 0,
          RECONCILING: 0,
          RUNNING: 0,
          RESTARTING: 0,
          INITIALIZING: 0,
          SUSPENDED: 0,
          TOTAL: 0
        };
        data.jobs.forEach(job => {
          statusCounts[job.state as keyof JobStatus] += 1;
          statusCounts['TOTAL'] += 1;
          for (const key in job.tasks) {
            const upperCaseKey = key.toUpperCase() as keyof TaskStatus;
            job.tasks[upperCaseKey] = job.tasks[key as keyof TaskStatus];
            delete job.tasks[key as keyof TaskStatus];
          }
          job.tasks['PENDING'] = job['pending-operators'] || 0;
          job.completed = ['FINISHED', 'FAILED', 'CANCELED'].indexOf(job.state) > -1;
        });
        data['status-counts'] = statusCounts;
        return data;
      }),
      catchError(rethrowOnlyNotFound)
    );
  }

  public loadExceptions(applicationId: string): Observable<ApplicationExceptions> {
    return this.httpClient.get<ApplicationExceptions>(
      `${this.configService.BASE_URL}/applications/${applicationId}/exceptions`
    );
  }
}
