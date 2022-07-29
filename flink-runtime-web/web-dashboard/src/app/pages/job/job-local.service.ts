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
import { Observable, ReplaySubject } from 'rxjs';
import { filter, map, withLatestFrom } from 'rxjs/operators';

import { JobDetailCorrect, NodesItemCorrect } from '@flink-runtime-web/interfaces';

export interface JobWithVertex {
  job: JobDetailCorrect;
  vertex: NodesItemCorrect;
}

@Injectable()
export class JobLocalService {
  /** Selected Metric Cache. */
  public readonly metricsCacheMap = new Map<string, string[]>();

  /** Current activated job. */
  private readonly jobDetail$ = new ReplaySubject<JobDetailCorrect>(1);

  /** Current activated vertex. */
  private readonly selectedVertex$ = new ReplaySubject<NodesItemCorrect | null>(1);

  constructor() {}

  /** Current activated job with vertex. */
  jobWithVertexChanges(): Observable<JobWithVertex> {
    return this.selectedVertex$.pipe(
      withLatestFrom(this.jobDetail$),
      map(data => {
        const [vertex, job] = data;
        return { vertex, job };
      }),
      filter((data): data is JobWithVertex => !!data.vertex)
    );
  }

  jobDetailChanges(): Observable<JobDetailCorrect> {
    return this.jobDetail$.asObservable();
  }

  setJobDetail(jobDetail: JobDetailCorrect): void {
    this.jobDetail$.next(jobDetail);
  }

  selectedVertexChanges(): Observable<NodesItemCorrect | null> {
    return this.selectedVertex$.asObservable();
  }

  setSelectedVertex(vertex: NodesItemCorrect | null): void {
    this.selectedVertex$.next(vertex);
  }
}
