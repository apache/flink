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

import { HttpErrorResponse } from '@angular/common/http';
import { defer, EMPTY, Observable, of } from 'rxjs';
import { catchError, map, switchMap, takeWhile } from 'rxjs/operators';

import { TaskManagerDetail } from '@flink-runtime-web/interfaces';

/** Outcome of polling a single TaskManager's detail. */
export interface TaskManagerDetailResult {
  detail?: TaskManagerDetail;
  notFound: boolean;
}

/**
 * Loads a single TaskManager's detail on every tick, stopping for good once the backend reports
 * the TaskManager is gone (404). A gone TaskManager never comes back (its id is unique per
 * registration), so re-polling it is futile; transient errors are ignored and keep polling.
 */
export function pollTaskManagerDetail(
  tick$: Observable<unknown>,
  load: () => Observable<TaskManagerDetail>
): Observable<TaskManagerDetailResult> {
  return defer(() => {
    let notFound = false;
    return tick$.pipe(
      takeWhile(() => !notFound),
      switchMap(() =>
        load().pipe(
          map(detail => ({ detail, notFound: false })),
          catchError((error: unknown) => {
            if (error instanceof HttpErrorResponse && error.status === 404) {
              notFound = true;
              return of({ detail: undefined, notFound: true });
            }
            return EMPTY;
          })
        )
      )
    );
  });
}
