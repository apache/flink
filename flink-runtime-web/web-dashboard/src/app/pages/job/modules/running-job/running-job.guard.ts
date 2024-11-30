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

import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { JobService } from '@flink-runtime-web/services';

@Injectable({
  providedIn: 'root'
})
export class RunningJobGuard implements CanActivate {
  constructor(private readonly jobService: JobService, private router: Router) {}

  canActivate(
    route: ActivatedRouteSnapshot,
    _: RouterStateSnapshot
  ): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    const jobId = route.params.jid;
    if (!jobId) {
      return false;
    }
    return this.jobService.loadJobs().pipe(
      map(jobs => {
        const jobItem = jobs.find(j => j.jid === jobId);
        if (!jobItem) {
          this.router.navigate(['/', 'job', 'running']).then();
          return false;
        }
        if (jobItem.completed) {
          this.router.navigate(['/', 'job', 'completed', jobId]).then();
          return false;
        }
        return true;
      })
    );
  }
}
