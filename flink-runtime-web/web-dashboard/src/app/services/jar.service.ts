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

import { HttpClient, HttpEvent, HttpParams, HttpRequest } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { BASE_URL } from 'config';
import { JarList, NodesItemCorrect, Plan, VerticesLink } from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class JarService {
  constructor(private readonly httpClient: HttpClient) {}

  public loadJarList(): Observable<JarList> {
    return this.httpClient.get<JarList>(`${BASE_URL}/jars`).pipe(
      catchError(() => {
        return of({
          address: '',
          error: true,
          files: []
        });
      })
    );
  }

  public uploadJar(fd: File): Observable<HttpEvent<unknown>> {
    const formData = new FormData();
    formData.append('jarfile', fd, fd.name);
    const req = new HttpRequest('POST', `${BASE_URL}/jars/upload`, formData, {
      reportProgress: true
    });
    return this.httpClient.request(req);
  }

  public deleteJar(jarId: string): Observable<void> {
    return this.httpClient.delete<void>(`${BASE_URL}/jars/${jarId}`);
  }

  public runJob(
    jarId: string,
    entryClass: string,
    parallelism: string,
    programArgs: string,
    savepointPath: string,
    allowNonRestoredState: string
  ): Observable<{ jobid: string }> {
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
    return this.httpClient.post<{ jobid: string }>(`${BASE_URL}/jars/${jarId}/run`, requestParam, { params });
  }

  public getPlan(
    jarId: string,
    entryClass: string,
    parallelism: string,
    programArgs: string
  ): Observable<{ nodes: NodesItemCorrect[]; links: VerticesLink[] }> {
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
    return this.httpClient.get<Plan>(`${BASE_URL}/jars/${jarId}/plan`, { params }).pipe(
      map(data => {
        const links: VerticesLink[] = [];
        let nodes: NodesItemCorrect[] = [];
        if (data.plan.nodes.length) {
          nodes = data.plan.nodes.map(node => {
            return {
              ...node,
              detail: undefined
            };
          });
          nodes.forEach(node => {
            if (node.inputs && node.inputs.length) {
              node.inputs.forEach(input => {
                links.push({ ...input, source: input.id, target: node.id, id: `${input.id}-${node.id}` });
              });
            }
          });
        }
        return { nodes, links };
      })
    );
  }
}
