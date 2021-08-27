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

import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { Injectable, Injector } from '@angular/core';
import { NzNotificationService } from 'ng-zorro-antd/notification';
import { throwError, Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { StatusService } from 'services';

@Injectable()
export class AppInterceptor implements HttpInterceptor {
  constructor(private injector: Injector) {}

  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    /**
     * Error response from below url should be ignored
     */
    const ignoreErrorUrlEndsList = ['checkpoints/config', 'checkpoints'];
    const ignoreErrorMessage = ['File not found.'];
    return next.handle(req).pipe(
      catchError(res => {
        const errorMessage = res && res.error && res.error.errors && res.error.errors[0];
        if (
          errorMessage &&
          ignoreErrorUrlEndsList.every(url => !res.url.endsWith(url)) &&
          ignoreErrorMessage.every(message => errorMessage !== message)
        ) {
          this.injector.get<StatusService>(StatusService).listOfErrorMessage.push(errorMessage);
          this.injector
            .get<NzNotificationService>(NzNotificationService)
            .info('Server Response Message:', errorMessage);
        }
        return throwError(res);
      })
    );
  }
}
