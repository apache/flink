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

import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { combineLatest, EMPTY, Observable, ReplaySubject } from 'rxjs';
import { catchError, filter, map, mergeMap, tap } from 'rxjs/operators';

import { BASE_URL } from 'config';
import {
  Checkpoint,
  CheckpointConfig,
  CheckpointDetail,
  CheckpointSubTask,
  JobBackpressure,
  JobConfig,
  JobDetail,
  JobDetailCorrect,
  JobException,
  JobFlameGraph,
  JobOverview,
  JobsItem,
  JobSubTask,
  JobSubTaskTime,
  JobVertexTaskManager,
  NodesItemCorrect,
  SubTaskAccumulators,
  TaskStatus,
  UserAccumulators,
  VerticesLink
} from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class JobService {
  /** Current activated job. */
  public readonly jobDetail$ = new ReplaySubject<JobDetailCorrect>(1);

  /** Current activated vertex. */
  public readonly selectedVertex$ = new ReplaySubject<NodesItemCorrect | null>(1);

  /** Current activated job with vertex. */
  public readonly jobWithVertex$ = combineLatest([this.jobDetail$, this.selectedVertex$]).pipe(
    map(data => {
      const [job, vertex] = data;
      return { job, vertex };
    }),
    filter(data => !!data.vertex)
  );

  /** Selected Metric Cache. */
  public readonly metricsCacheMap = new Map<string, string[]>();

  constructor(private readonly httpClient: HttpClient) {}

  /**
   * Uses the non REST-compliant GET yarn-cancel handler which is available in addition to the
   * proper BASE_URL + "jobs/" + jobid + "?mode=cancel"
   */
  public cancelJob(jobId: string): Observable<void> {
    return this.httpClient.get<void>(`${BASE_URL}/jobs/${jobId}/yarn-cancel`);
  }

  public loadJobs(): Observable<JobsItem[]> {
    return this.httpClient.get<JobOverview>(`${BASE_URL}/jobs/overview`).pipe(
      map(data => {
        data.jobs.forEach(job => {
          for (const key in job.tasks) {
            const upperCaseKey = key.toUpperCase() as keyof TaskStatus;
            job.tasks[upperCaseKey] = job.tasks[key as keyof TaskStatus];
            delete job.tasks[key as keyof TaskStatus];
          }
          job.completed = ['FINISHED', 'FAILED', 'CANCELED'].indexOf(job.state) > -1;
        });
        return data.jobs || [];
      }),
      catchError(() => EMPTY)
    );
  }

  public loadJobConfig(jobId: string): Observable<JobConfig> {
    return this.httpClient.get<JobConfig>(`${BASE_URL}/jobs/${jobId}/config`);
  }

  public loadJob(jobId: string): Observable<JobDetailCorrect> {
    return this.httpClient.get<JobDetail>(`${BASE_URL}/jobs/${jobId}`).pipe(
      map(job => this.convertJob(job)),
      tap(job => {
        this.jobDetail$.next(job);
      })
    );
  }

  public loadAccumulators(
    jobId: string,
    vertexId: string
  ): Observable<{ main: UserAccumulators[]; subtasks: SubTaskAccumulators[] }> {
    return this.httpClient
      .get<{ 'user-accumulators': UserAccumulators[] }>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/accumulators`)
      .pipe(
        mergeMap(data => {
          const accumulators = data['user-accumulators'];
          return this.httpClient
            .get<{ subtasks: SubTaskAccumulators[] }>(
              `${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/subtasks/accumulators`
            )
            .pipe(
              map(item => {
                const subtaskAccumulators = item.subtasks;
                return {
                  main: accumulators,
                  subtasks: subtaskAccumulators
                };
              })
            );
        })
      );
  }

  public loadExceptions(jobId: string, maxExceptions: number): Observable<JobException> {
    return this.httpClient.get<JobException>(`${BASE_URL}/jobs/${jobId}/exceptions?maxExceptions=${maxExceptions}`);
  }

  public loadOperatorBackPressure(jobId: string, vertexId: string): Observable<JobBackpressure> {
    return this.httpClient.get<JobBackpressure>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/backpressure`);
  }

  public loadOperatorFlameGraph(jobId: string, vertexId: string, type: string): Observable<JobFlameGraph> {
    return this.httpClient.get<JobFlameGraph>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/flamegraph?type=${type}`);
  }

  public loadSubTasks(jobId: string, vertexId: string): Observable<JobSubTask[]> {
    return this.httpClient
      .get<{ subtasks: JobSubTask[] }>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}`)
      .pipe(map(data => (data && data.subtasks) || []));
  }

  public loadSubTaskTimes(jobId: string, vertexId: string): Observable<JobSubTaskTime> {
    return this.httpClient.get<JobSubTaskTime>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/subtasktimes`);
  }

  public loadTaskManagers(jobId: string, vertexId: string): Observable<JobVertexTaskManager> {
    return this.httpClient.get<JobVertexTaskManager>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/taskmanagers`);
  }

  public loadCheckpointStats(jobId: string): Observable<Checkpoint> {
    return this.httpClient.get<Checkpoint>(`${BASE_URL}/jobs/${jobId}/checkpoints`);
  }

  public loadCheckpointConfig(jobId: string): Observable<CheckpointConfig> {
    return this.httpClient.get<CheckpointConfig>(`${BASE_URL}/jobs/${jobId}/checkpoints/config`);
  }

  public loadCheckpointDetails(jobId: string, checkPointId: number): Observable<CheckpointDetail> {
    return this.httpClient.get<CheckpointDetail>(`${BASE_URL}/jobs/${jobId}/checkpoints/details/${checkPointId}`);
  }

  public loadCheckpointSubtaskDetails(
    jobId: string,
    checkPointId: number,
    vertexId: string
  ): Observable<CheckpointSubTask> {
    return this.httpClient.get<CheckpointSubTask>(
      `${BASE_URL}/jobs/${jobId}/checkpoints/details/${checkPointId}/subtasks/${vertexId}`
    );
  }

  /** nodes to nodes links in order to generate graph */
  private convertJob(job: JobDetail): JobDetailCorrect {
    const links: VerticesLink[] = [];
    let nodes: NodesItemCorrect[] = [];
    if (job.plan.nodes.length) {
      nodes = job.plan.nodes.map(node => {
        let detail;
        if (job.vertices && job.vertices.length) {
          detail = job.vertices.find(vertex => vertex.id === node.id);
        }
        return {
          ...node,
          detail
        };
      });
      nodes.forEach(node => {
        if (node.inputs && node.inputs.length) {
          node.inputs.forEach(input => {
            links.push({ ...input, source: input.id, target: node.id, id: `${input.id}-${node.id}` });
          });
        }
      });
      const listOfVerticesId = job.vertices.map(item => item.id);
      nodes.sort((pre, next) => listOfVerticesId.indexOf(pre.id) - listOfVerticesId.indexOf(next.id));
    }
    return {
      ...job,
      plan: {
        ...job.plan,
        nodes,
        links
      }
    };
  }
}
