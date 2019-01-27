/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ConfigService } from './config.service';
import { forkJoin, ReplaySubject, Subject } from 'rxjs';
import { flatMap, map } from 'rxjs/operators';
import {
  JobBackpressureInterface,
  JobOverviewInterface,
  JobPendingSlotsInterface,
  JobsItemInterface,
  NodesItemCorrectInterface,
  NodesItemInterface,
  VerticesDetailInterface
} from 'flink-interfaces';
import {
  CheckPointInterface,
  CheckPointConfigInterface,
  CheckPointDetailInterface,
  CheckPointSubTaskInterface,
  JobExceptionInterface,
  JobConfigInterface,
  JobDetailInterface,
  JobDetailCorrectInterface,
  JobSubTaskTimeInterface,
  VerticesItemInterface,
  JobVertexTaskManagerInterface,
  VertexTaskManagerDetailInterface,
  JobSubTaskInterface
} from 'flink-interfaces';

@Injectable({
  providedIn: 'root'
})
export class JobService {
  jobPrefix = 'jobs';
  selectedVertexNode$ = new ReplaySubject<NodesItemCorrectInterface>(1);
  jobDetail: JobDetailCorrectInterface;
  jobDetail$ = new ReplaySubject<JobDetailCorrectInterface>(1);
  jobDetailLatest$ = new Subject<JobDetailCorrectInterface>();
  listOfNavigation = [
    { title: 'Detail', pathOrParam: 'detail' },
    { title: 'SubTasks', pathOrParam: 'subtasks' },
    { title: 'TaskManagers', pathOrParam: 'taskmanagers' },
    { title: 'Watermarks', pathOrParam: 'watermarks' },
    { title: 'Accumulators', pathOrParam: 'accumulators' },
    { title: 'BackPressure', pathOrParam: 'backpressure' },
    { title: 'Metrics', pathOrParam: 'metrics' }
  ];

  constructor(private httpClient: HttpClient, private configService: ConfigService) {
  }

  setJobDetail(data: JobDetailCorrectInterface) {
    this.jobDetail = data;
    this.jobDetail$.next(data);
    this.jobDetailLatest$.next(data);
  }

  cancelJob(jobId) {
    return this.httpClient.get(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/yarn-cancel`);
  }

  stopJob(jobId) {
    return this.httpClient.get(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/yarn-stop`);
  }

  loadJobs() {
    return this.httpClient.get<JobOverviewInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/overview`).pipe(
      map(data => {
        data.jobs.forEach(job => {
          for (const key in job.tasks) {
            const upperCaseKey = key.toUpperCase();
            job.tasks[ upperCaseKey ] = job.tasks[ key ];
            delete job.tasks[ key ];
          }
          job.completed = [ 'FINISHED', 'FAILED', 'CANCELED' ].indexOf(job.state) > -1;
          this.setEndTimes(job);
        });
        return data.jobs || [];
      })
    );
  }

  loadJobConfig(jobId) {
    return this.httpClient.get<JobConfigInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/config`);
  }

  loadJobWithVerticesDetail(jobId) {
    return forkJoin(
      this.loadJob(jobId),
      this.loadJobVerticesDetail(jobId)
    ).pipe(map(([ job, vertices ]) => ({
      ...job,
      verticesDetail: vertices
    })));
  }

  fillEmptyOperators(nodes: NodesItemInterface[], verticesDetail: VerticesDetailInterface): VerticesDetailInterface {
    if (verticesDetail.operators.length === 0) {
      verticesDetail.operators = [];
      nodes.forEach(node => {
        verticesDetail.operators.push({
          operator_id: node.id,
          vertex_id  : node.id,
          name       : node.description,
          metric_name: null,
          virtual    : true,
          inputs     : node.inputs ? node.inputs.map(i => {
            return {
              operator_id: i.id,
              partitioner: i.ship_strategy,
              type_number: i.num
            };
          }) : []
        });
      });
    }
    return verticesDetail;
  }

  loadJob(jobId) {
    return this.httpClient.get<JobDetailInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}`).pipe(
      map(job => (job && Object.keys(job).length) ? this.convertJob(job) : null)
    );
  }

  loadJobVerticesDetail(jobId: string) {
    return this.httpClient.get<VerticesDetailInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/details`);
  }

  loadAccumulators(jobId, vertexId) {
    return this.httpClient.get(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/${vertexId}/accumulators`
    ).pipe(flatMap(data => {
      const accumulators = data[ 'user-accumulators' ];
      return this.httpClient.get(
        `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/${vertexId}/subtasks/accumulators`
      ).pipe(map(item => {
        const subtaskAccumulators = item[ 'subtasks' ];
        return {
          main    : accumulators,
          subtasks: subtaskAccumulators
        };
      }));
    }));
  }

  loadExceptions(jobId) {
    return this.httpClient.get<JobExceptionInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/exceptions`);
  }

  loadOperatorBackPressure(jobId, vertexId) {
    return this.httpClient.get<JobBackpressureInterface>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/${vertexId}/backpressure`
    );
  }

  loadSubTasks(jobId, vertexId) {
    return this.httpClient.get<{ subtasks: JobSubTaskInterface[] }>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/${vertexId}`
    ).pipe(map(
      item => {
        item.subtasks.forEach(task => {
          this.setEndTimes(task);
          if (task.metrics) {
            if (task.metrics) {
              this.setMetricNull(task.metrics);
            }
          }
        });
        return item.subtasks;
      }
    ));
  }

  loadSubTaskTimes(jobId, vertexId) {
    return this.httpClient.get<JobSubTaskTimeInterface>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/${vertexId}/subtasktimes`
    );
  }

  loadTaskManagers(jobId, vertexId) {
    return this.httpClient.get<JobVertexTaskManagerInterface>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/vertices/${vertexId}/taskmanagers`
    ).pipe(map(item => {
      if (item.taskmanagers) {
        item.taskmanagers.forEach(taskManager => {
          this.setEndTimes(taskManager);
        });
      }
      return item;
    }));
  }


  loadCheckpointStats(jobId) {
    return this.httpClient.get<CheckPointInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/checkpoints`);
  }

  loadCheckpointConfig(jobId) {
    return this.httpClient.get<CheckPointConfigInterface>(`${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/checkpoints/config`);
  }

  loadCheckpointDetails(jobId, checkPointId) {
    return this.httpClient.get<CheckPointDetailInterface>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/checkpoints/details/${checkPointId}`
    );
  }

  loadCheckpointSubtaskDetails(jobId, checkPointId, vertexId) {
    return this.httpClient.get<CheckPointSubTaskInterface>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/checkpoints/details/${checkPointId}/subtasks/${vertexId}`
    );
  }

  loadPendingSlots(jobId) {
    return this.httpClient.get<JobPendingSlotsInterface>(
      `${this.configService.BASE_URL}/${this.jobPrefix}/${jobId}/pendingslotrequest`
    ).pipe(map(data => {
      if (data[ 'pending-slot-requests' ]) {
        data[ 'pending-slot-requests' ].forEach(item => {
          if (item.resource_profile) {
            this.setMetricNull(item.resource_profile);
          }
        });
      }
      return data;
    }));
  }

  convertJob(job: JobDetailInterface): JobDetailCorrectInterface {
    const links = [];
    if (job.vertices) {
      job.vertices.forEach(vertex => {
        this.setEndTimes(vertex);
        if (vertex.metrics) {
          this.setMetricNull(vertex.metrics);
        }
      });
    }
    if (job.plan.nodes.length) {
      job.plan.nodes.forEach(node => {
        let detail = {} as VerticesItemInterface;
        if (job.vertices && job.vertices.length) {
          detail = job.vertices.find(vertex => {
            return vertex.id === node.id;
          });
        }
        node[ 'detail' ] = detail;
        if (node.inputs && node.inputs.length) {
          node.inputs.forEach(input => {
            links.push({ ...input, source: input.id, target: node.id, id: `${input.id}-${node.id}` });
          });
        }
      });
    }
    job.plan[ 'links' ] = links;
    return job as JobDetailCorrectInterface;
  }

  private setEndTimes(item: JobsItemInterface | VerticesItemInterface | JobSubTaskInterface | VertexTaskManagerDetailInterface) {
    if (item[ 'end-time' ] <= -1) {
      item[ 'end-time' ] = (item[ 'start-time' ] || item[ 'start_time' ]) + item.duration;
    }
  }

  private setMetricNull(metrics) {
    for (const key in metrics) {
      if (metrics[ key ] === -1) {
        metrics[ key ] = null;
      }
    }
  }
}
