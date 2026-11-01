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

import { HttpContextToken, HttpErrorResponse, HttpStatusCode } from '@angular/common/http';
import { EMPTY, Observable, throwError } from 'rxjs';

/**
 * Marks a request for which a 404 is an expected outcome that the caller handles itself, so the
 * global interceptor does not surface it as a server error notification. Any other error is
 * still surfaced as usual.
 */
export const EXPECTED_NOT_FOUND = new HttpContextToken<boolean>(() => false);

export function isNotFound(error: unknown): boolean {
  return error instanceof HttpErrorResponse && error.status === HttpStatusCode.NotFound;
}

/**
 * A catchError handler that re-throws a 404, so the caller can react to a resource that is gone,
 * and completes silently on any other error.
 */
export function rethrowOnlyNotFound(error: unknown): Observable<never> {
  return isNotFound(error) ? throwError(() => error) : EMPTY;
}
