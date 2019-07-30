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
import {
  CheckPointConfigInterface,
  CheckPointDetailInterface,
  CheckPointInterface,
  CheckPointSubTaskInterface,
  JobBackpressureInterface,
  JobConfigInterface,
  JobDetailCorrectInterface,
  JobDetailInterface,
  JobExceptionInterface,
  JobOverviewInterface,
  JobSubTaskInterface,
  JobSubTaskTimeInterface,
  JobVertexTaskManagerInterface,
  NodesItemCorrectInterface,
  SubTaskAccumulatorsInterface,
  TaskStatusInterface,
  UserAccumulatorsInterface,
  VerticesLinkInterface
} from 'interfaces';
import { combineLatest, EMPTY, ReplaySubject } from 'rxjs';
import { catchError, filter, flatMap, map, tap } from 'rxjs/operators';
import { BASE_URL } from 'config';

@Injectable({
  providedIn: 'root'
})
export class JobService {
  /**
   * Current activated job
   */
  jobDetail$ = new ReplaySubject<JobDetailCorrectInterface>(1);
  /**
   * Current activated vertex
   */
  selectedVertex$ = new ReplaySubject<NodesItemCorrectInterface | null>(1);
  /**
   * Current activated job with vertex
   */
  jobWithVertex$ = combineLatest(this.jobDetail$, this.selectedVertex$).pipe(
    map(data => {
      const [job, vertex] = data;
      return { job, vertex };
    }),
    filter(data => !!data.vertex)
  );
  /**
   * Selected Metric Cache
   */
  metricsCacheMap = new Map();

  constructor(private httpClient: HttpClient) {}

  /**
   * Uses the non REST-compliant GET yarn-cancel handler which is available in addition to the
   * proper BASE_URL + "jobs/" + jobid + "?mode=cancel"
   * @param jobId
   */
  cancelJob(jobId: string) {
    return this.httpClient.get(`${BASE_URL}/jobs/${jobId}/yarn-cancel`);
  }

  /**
   * Get job list
   */
  loadJobs() {
    return this.httpClient.get<JobOverviewInterface>(`${BASE_URL}/jobs/overview`).pipe(
      map(data => {
        data.jobs.forEach(job => {
          for (const key in job.tasks) {
            const upperCaseKey = key.toUpperCase() as (keyof TaskStatusInterface);
            job.tasks[upperCaseKey] = job.tasks[key as (keyof TaskStatusInterface)];
            delete job.tasks[key as (keyof TaskStatusInterface)];
          }
          job.completed = ['FINISHED', 'FAILED', 'CANCELED'].indexOf(job.state) > -1;
        });
        return data.jobs || [];
      }),
      catchError(() => EMPTY)
    );
  }

  /**
   * Load job config
   * @param jobId
   */
  loadJobConfig(jobId: string) {
    return this.httpClient.get<JobConfigInterface>(`${BASE_URL}/jobs/${jobId}/config`);
  }

  /**
   * Load single job detail
   * @param jobId
   */
  loadJob(jobId: string) {
    return this.httpClient.get<JobDetailInterface>(`${BASE_URL}/jobs/${jobId}`).pipe(
      map(job => this.convertJob(job)),
      tap(job => {
        this.jobDetail$.next(job);
      }),
      catchError(() => EMPTY)
    );
  }

  /**
   * Get vertex & subtask accumulators
   * @param jobId
   * @param vertexId
   */
  loadAccumulators(jobId: string, vertexId: string) {
    return this.httpClient
      .get<{ 'user-accumulators': UserAccumulatorsInterface[] }>(
        `${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/accumulators`
      )
      .pipe(
        flatMap(data => {
          const accumulators = data['user-accumulators'];
          return this.httpClient
            .get<{ subtasks: SubTaskAccumulatorsInterface[] }>(
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

  /**
   * Get job exception
   * @param jobId
   */
  loadExceptions(jobId: string) {
    return this.httpClient.get<JobExceptionInterface>(`${BASE_URL}/jobs/${jobId}/exceptions`);
  }

  /**
   * Get vertex back pressure
   * @param jobId
   * @param vertexId
   */
  loadOperatorBackPressure(jobId: string, vertexId: string) {
    return this.httpClient.get<JobBackpressureInterface>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/backpressure`);
  }

  /**
   * Get vertex subtask
   * @param jobId
   * @param vertexId
   */
  loadSubTasks(jobId: string, vertexId: string) {
    return this.httpClient
      .get<{ subtasks: JobSubTaskInterface[] }>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}`)
      .pipe(map(data => (data && data.subtasks) || []));
  }

  /**
   * Get subtask timeline
   * @param jobId
   * @param vertexId
   */
  loadSubTaskTimes(jobId: string, vertexId: string) {
    return this.httpClient.get<JobSubTaskTimeInterface>(`${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/subtasktimes`);
  }

  /**
   * Get vertex task manager list
   * @param jobId
   * @param vertexId
   */
  loadTaskManagers(jobId: string, vertexId: string) {
    return this.httpClient.get<JobVertexTaskManagerInterface>(
      `${BASE_URL}/jobs/${jobId}/vertices/${vertexId}/taskmanagers`
    );
  }

  /**
   * Get check point status
   * @param jobId
   */
  loadCheckpointStats(jobId: string) {
    return this.httpClient.get<CheckPointInterface>(`${BASE_URL}/jobs/${jobId}/checkpoints`);
  }

  /**
   * Get check point configuration
   * @param jobId
   */
  loadCheckpointConfig(jobId: string) {
    return this.httpClient.get<CheckPointConfigInterface>(`${BASE_URL}/jobs/${jobId}/checkpoints/config`);
  }

  /**
   * get check point detail
   * @param jobId
   * @param checkPointId
   */
  loadCheckpointDetails(jobId: string, checkPointId: number) {
    return this.httpClient.get<CheckPointDetailInterface>(
      `${BASE_URL}/jobs/${jobId}/checkpoints/details/${checkPointId}`
    );
  }

  /**
   * get subtask check point detail
   * @param jobId
   * @param checkPointId
   * @param vertexId
   */
  loadCheckpointSubtaskDetails(jobId: string, checkPointId: number, vertexId: string) {
    return this.httpClient.get<CheckPointSubTaskInterface>(
      `${BASE_URL}/jobs/${jobId}/checkpoints/details/${checkPointId}/subtasks/${vertexId}`
    );
  }

  /**
   * nodes to nodes links in order to generate graph
   * @param job
   */
  private convertJob(job: JobDetailInterface): JobDetailCorrectInterface {
    const links: VerticesLinkInterface[] = [];
    let nodes: NodesItemCorrectInterface[] = [];
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
