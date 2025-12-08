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

import { ApplicationService } from '@flink-runtime-web/services';

@Injectable({
  providedIn: 'root'
})
export class RunningApplicationGuard implements CanActivate {
  constructor(private readonly applicationService: ApplicationService, private router: Router) {}

  canActivate(
    route: ActivatedRouteSnapshot,
    _: RouterStateSnapshot
  ): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    const applicationId = route.params['id'];
    if (!applicationId) {
      return false;
    }
    return this.applicationService.loadApplications().pipe(
      map(applications => {
        const applicationItem = applications.find(application => application.id === applicationId);
        if (!applicationItem) {
          this.router.navigate(['/', 'application', 'running']).then();
          return false;
        }
        if (applicationItem.completed) {
          this.router.navigate(['/', 'application', 'completed', applicationId]).then();
          return false;
        }
        return true;
      })
    );
  }
}
