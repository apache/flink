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

import { HttpClient, HttpRequest, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BASE_URL } from 'config';
import { ArtifactListInterface, NodesItemCorrectInterface, PlanInterface, VerticesLinkInterface } from 'interfaces';
import { of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class ArtifactService {
  /**
   * Get uploaded artifact list
   */
  loadArtifactList() {
    return this.httpClient.get<ArtifactListInterface>(`${BASE_URL}/artifacts`).pipe(
      catchError(() => {
        return of({
          address: '',
          error: true,
          files: []
        });
      })
    );
  }

  /**
   * Upload artifact
   * @param fd
   */
  uploadArtifact(fd: File) {
    const formData = new FormData();
    formData.append('jarfile', fd, fd.name);
    const req = new HttpRequest('POST', `${BASE_URL}/artifacts/upload`, formData, {
      reportProgress: true
    });
    return this.httpClient.request(req);
  }

  /**
   * Delete artifact
   * @param artifactId
   */
  deleteArtifact(artifactId: string) {
    return this.httpClient.delete(`${BASE_URL}/artifacts/${artifactId}`);
  }

  /**
   * Run job
   * @param artifact
   * @param entryClass
   * @param parallelism
   * @param programArgs
   * @param savepointPath
   * @param allowNonRestoredState
   */
  runJob(
    artifactId: string,
    entryClass: string,
    parallelism: string,
    programArgs: string,
    savepointPath: string,
    allowNonRestoredState: string
  ) {
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
    return this.httpClient.post<{ jobid: string }>(`${BASE_URL}/artifacts/${artifactId}/run`, requestParam, { params });
  }

  /**
   * Get plan json from artifact
   * @param artifactId
   * @param entryClass
   * @param parallelism
   * @param programArgs
   */
  getPlan(artifactId: string, entryClass: string, parallelism: string, programArgs: string) {
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
    return this.httpClient.get<PlanInterface>(`${BASE_URL}/artifacts/${artifactId}/plan`, { params }).pipe(
      map(data => {
        const links: VerticesLinkInterface[] = [];
        let nodes: NodesItemCorrectInterface[] = [];
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

  constructor(private httpClient: HttpClient) {}
}
