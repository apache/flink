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
import { EMPTY, forkJoin, mergeMap, Observable } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import {
  Checkpoint,
  CheckpointConfig,
  CheckpointDetail,
  CheckpointSubTask,
  JobAccumulators,
  JobBackpressure,
  JobConfig,
  JobDetail,
  JobDetailCorrect,
  JobException,
  JobFlameGraph,
  JobOverview,
  JobsItem,
  JobSubTaskTime,
  JobVertexTaskManager,
  NodesItemCorrect,
  SubTaskAccumulators,
  TaskStatus,
  UserAccumulators,
  VerticesLink,
  JobVertexSubTaskDetail
} from '@flink-runtime-web/interfaces';
import { JobResourceRequirements } from '@flink-runtime-web/interfaces/job-resource-requirements';

import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})
export class JobService {
  constructor(private readonly httpClient: HttpClient, private readonly configService: ConfigService) {}

  /**
   * Uses the non REST-compliant GET yarn-cancel handler which is available in addition to the
   * proper BASE_URL + "jobs/" + jobid + "?mode=cancel"
   */
  public cancelJob(jobId: string): Observable<void> {
    return this.httpClient.get<void>(`${this.configService.BASE_URL}/jobs/${jobId}/yarn-cancel`);
  }

  public loadJobs(): Observable<JobsItem[]> {
    return this.httpClient.get<JobOverview>(`${this.configService.BASE_URL}/jobs/overview`).pipe(
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
    return this.httpClient.get<JobConfig>(`${this.configService.BASE_URL}/jobs/${jobId}/config`);
  }

  public loadJob(jobId: string): Observable<JobDetailCorrect> {
    return this.httpClient
      .get<JobDetail>(`${this.configService.BASE_URL}/jobs/${jobId}`)
      .pipe(map(job => this.convertJob(job)));
  }

  public loadAccumulators(jobId: string, vertexId: string): Observable<JobAccumulators> {
    return forkJoin([
      this.httpClient.get<{ 'user-accumulators': UserAccumulators[] }>(
        `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/accumulators`
      ),
      this.httpClient.get<{ subtasks: SubTaskAccumulators[] }>(
        `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/subtasks/accumulators`
      )
    ]).pipe(
      map(([user, subtask]) => ({
        main: user['user-accumulators'],
        subtasks: subtask.subtasks
      }))
    );
  }

  public loadExceptions(jobId: string, maxExceptions: number): Observable<JobException> {
    return this.httpClient.get<JobException>(
      `${this.configService.BASE_URL}/jobs/${jobId}/exceptions?maxExceptions=${maxExceptions}`
    );
  }

  public loadOperatorBackPressure(jobId: string, vertexId: string): Observable<JobBackpressure> {
    return this.httpClient.get<JobBackpressure>(
      `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/backpressure`
    );
  }

  public loadOperatorFlameGraph(jobId: string, vertexId: string, type: string): Observable<JobFlameGraph> {
    return this.httpClient.get<JobFlameGraph>(
      `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/flamegraph?type=${type}`
    );
  }

  public loadOperatorFlameGraphForSingleSubtask(
    jobId: string,
    vertexId: string,
    type: string,
    subtaskIndex: string
  ): Observable<JobFlameGraph> {
    return this.httpClient.get<JobFlameGraph>(
      `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/flamegraph?type=${type}&subtaskindex=${subtaskIndex}`
    );
  }

  public loadSubTasks(jobId: string, vertexId: string): Observable<JobVertexSubTaskDetail> {
    return this.httpClient.get<JobVertexSubTaskDetail>(
      `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}`
    );
  }

  public loadSubTaskTimes(jobId: string, vertexId: string): Observable<JobSubTaskTime> {
    return this.httpClient.get<JobSubTaskTime>(
      `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/subtasktimes`
    );
  }

  public loadTaskManagers(jobId: string, vertexId: string): Observable<JobVertexTaskManager> {
    return this.httpClient.get<JobVertexTaskManager>(
      `${this.configService.BASE_URL}/jobs/${jobId}/vertices/${vertexId}/taskmanagers`
    );
  }

  public loadCheckpointStats(jobId: string): Observable<Checkpoint> {
    return this.httpClient.get<Checkpoint>(`${this.configService.BASE_URL}/jobs/${jobId}/checkpoints`);
  }

  public loadCheckpointConfig(jobId: string): Observable<CheckpointConfig> {
    return this.httpClient.get<CheckpointConfig>(`${this.configService.BASE_URL}/jobs/${jobId}/checkpoints/config`);
  }

  public loadCheckpointDetails(jobId: string, checkPointId: number): Observable<CheckpointDetail> {
    return this.httpClient.get<CheckpointDetail>(
      `${this.configService.BASE_URL}/jobs/${jobId}/checkpoints/details/${checkPointId}`
    );
  }

  public loadCheckpointSubtaskDetails(
    jobId: string,
    checkPointId: number,
    vertexId: string
  ): Observable<CheckpointSubTask> {
    return this.httpClient.get<CheckpointSubTask>(
      `${this.configService.BASE_URL}/jobs/${jobId}/checkpoints/details/${checkPointId}/subtasks/${vertexId}`
    );
  }

  public loadJobResourceRequirements(jobId: string): Observable<JobResourceRequirements> {
    return this.httpClient.get<JobResourceRequirements>(
      `${this.configService.BASE_URL}/jobs/${jobId}/resource-requirements`
    );
  }

  public changeDesiredParallelism(jobId: string, desiredParallelism: Map<string, number>): Observable<void> {
    return this.loadJobResourceRequirements(jobId)
      .pipe(
        map(jobResourceRequirements => {
          for (const vertexId in jobResourceRequirements) {
            const newUpperBound = desiredParallelism.get(vertexId);
            if (newUpperBound != undefined) {
              jobResourceRequirements[vertexId].parallelism.upperBound = newUpperBound;
            }
          }
          return jobResourceRequirements;
        })
      )
      .pipe(
        mergeMap(jobResourceRequirements => {
          return this.httpClient.put<void>(
            `${this.configService.BASE_URL}/jobs/${jobId}/resource-requirements`,
            jobResourceRequirements
          );
        })
      );
  }

  /** nodes to nodes links in order to generate graph */
  private convertJob(job: JobDetail): JobDetailCorrect {
    const links: VerticesLink[] = [];
    let nodes: NodesItemCorrect[] = [];
    if (job.plan?.nodes?.length) {
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
