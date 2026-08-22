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
import { Subject, of, throwError } from 'rxjs';

import { TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { describe, expect, it, vi } from 'vitest';

import { pollTaskManagerDetail, TaskManagerDetailResult } from './task-manager-detail-poll';

const detail = { id: 'tm-1' } as unknown as TaskManagerDetail;

describe('pollTaskManagerDetail', () => {
  it('emits the detail on every successful tick', () => {
    const tick$ = new Subject<void>();
    const results: TaskManagerDetailResult[] = [];
    pollTaskManagerDetail(tick$, () => of(detail)).subscribe(result => results.push(result));

    tick$.next();
    tick$.next();

    expect(results).toEqual([
      { detail, notFound: false },
      { detail, notFound: false }
    ]);
  });

  it('reports notFound and stops polling once the TaskManager is gone (404)', () => {
    const tick$ = new Subject<void>();
    const load = vi.fn(() => throwError(() => new HttpErrorResponse({ status: 404 })));
    const results: TaskManagerDetailResult[] = [];
    let completed = false;
    pollTaskManagerDetail(tick$, load).subscribe({
      next: result => results.push(result),
      complete: () => (completed = true)
    });

    tick$.next();
    tick$.next();

    expect(results).toEqual([{ detail: undefined, notFound: true }]);
    expect(load).toHaveBeenCalledTimes(1);
    expect(completed).toBe(true);
  });

  it('ignores a transient (non-404) error and keeps polling', () => {
    const tick$ = new Subject<void>();
    const load = vi
      .fn()
      .mockReturnValueOnce(throwError(() => new HttpErrorResponse({ status: 503 })))
      .mockReturnValueOnce(of(detail));
    const results: TaskManagerDetailResult[] = [];
    pollTaskManagerDetail(tick$, load).subscribe(result => results.push(result));

    tick$.next();
    tick$.next();

    expect(results).toEqual([{ detail, notFound: false }]);
    expect(load).toHaveBeenCalledTimes(2);
  });
});
