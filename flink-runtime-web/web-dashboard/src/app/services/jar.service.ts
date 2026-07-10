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

import { HttpClient, HttpEvent, HttpRequest } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import {
  JarList,
  JarPlanRequestBody,
  JarRunRequestBody,
  NodesItemCorrect,
  Plan,
  PlanDetail,
  VerticesLink
} from '@flink-runtime-web/interfaces';

import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})
export class JarService {
  constructor(private readonly httpClient: HttpClient, private readonly configService: ConfigService) {}

  public loadJarList(): Observable<JarList> {
    return this.httpClient.get<JarList>(`${this.configService.BASE_URL}/jars`).pipe(
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
    const req = new HttpRequest('POST', `${this.configService.BASE_URL}/jars/upload`, formData, {
      reportProgress: true
    });
    return this.httpClient.request(req);
  }

  public deleteJar(jarId: string): Observable<void> {
    return this.httpClient.delete<void>(`${this.configService.BASE_URL}/jars/${jarId}`);
  }

  public runJob(
    jarId: string,
    entryClass: string,
    parallelism: string,
    programArgs: string,
    savepointPath: string,
    allowNonRestoredState: string
  ): Observable<{ jobid: string }> {
    const requestBody: JarRunRequestBody = this.buildBaseRequestBody(entryClass, parallelism, programArgs);
    if (savepointPath) {
      requestBody.savepointPath = savepointPath;
    }
    if (allowNonRestoredState) {
      requestBody.allowNonRestoredState = true;
    }
    return this.httpClient.post<{ jobid: string }>(`${this.configService.BASE_URL}/jars/${jarId}/run`, requestBody);
  }

  public getPlan(jarId: string, entryClass: string, parallelism: string, programArgs: string): Observable<PlanDetail> {
    const requestBody: JarPlanRequestBody = this.buildBaseRequestBody(entryClass, parallelism, programArgs);
    return this.httpClient.post<Plan>(`${this.configService.BASE_URL}/jars/${jarId}/plan`, requestBody).pipe(
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

  private buildBaseRequestBody(entryClass: string, parallelism: string, programArgs: string): JarPlanRequestBody {
    const requestBody: JarPlanRequestBody = {};
    if (entryClass) {
      requestBody.entryClass = entryClass;
    }
    if (parallelism) {
      requestBody.parallelism = parseInt(parallelism, 10);
    }
    if (programArgs) {
      requestBody.programArgsList = this.parseArgs(programArgs);
    }
    return requestBody;
  }

  private parseArgs(programArgs: string): string[] {
    const args: string[] = [];
    let current = '';
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let hasQuote = false;

    for (let i = 0; i < programArgs.length; i++) {
      const char = programArgs[i];
      if (char === "'" && !inDoubleQuote) {
        inSingleQuote = !inSingleQuote;
        hasQuote = true;
      } else if (char === '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
        hasQuote = true;
      } else if (char === ' ' && !inSingleQuote && !inDoubleQuote) {
        if (current.length > 0 || hasQuote) {
          args.push(current);
          current = '';
          hasQuote = false;
        }
      } else {
        current += char;
      }
    }
    if (current.length > 0 || hasQuote) {
      args.push(current);
    }
    return args;
  }
}
