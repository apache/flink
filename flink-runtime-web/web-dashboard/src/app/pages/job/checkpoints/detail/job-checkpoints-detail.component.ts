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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnInit } from '@angular/core';
import {
  CheckPointCompletedStatisticsInterface,
  CheckPointDetailInterface,
  JobDetailCorrectInterface,
  VerticesItemInterface,
  CheckPointConfigInterface
} from 'interfaces';
import { forkJoin } from 'rxjs';
import { first } from 'rxjs/operators';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-checkpoints-detail',
  templateUrl: './job-checkpoints-detail.component.html',
  styleUrls: ['./job-checkpoints-detail.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobCheckpointsDetailComponent implements OnInit {
  innerCheckPoint: CheckPointCompletedStatisticsInterface;
  jobDetail: JobDetailCorrectInterface;
  checkPointType: string;

  @Input()
  set checkPoint(value) {
    this.innerCheckPoint = value;
    this.refresh();
  }

  get checkPoint() {
    return this.innerCheckPoint;
  }

  checkPointDetail: CheckPointDetailInterface;
  checkPointConfig: CheckPointConfigInterface;
  listOfVertex: VerticesItemInterface[] = [];
  isLoading = true;

  trackVertexBy(_: number, node: VerticesItemInterface) {
    return node.id;
  }

  refresh() {
    this.isLoading = true;
    if (this.jobDetail && this.jobDetail.jid) {
      forkJoin([
        this.jobService.loadCheckpointConfig(this.jobDetail.jid),
        this.jobService.loadCheckpointDetails(this.jobDetail.jid, this.checkPoint.id)
      ]).subscribe(
        ([config, detail]) => {
          this.checkPointConfig = config;
          this.checkPointDetail = detail;
          if (this.checkPointDetail.checkpoint_type === 'CHECKPOINT') {
            if (this.checkPointConfig.unaligned_checkpoints) {
              this.checkPointType = 'unaligned checkpoint';
            } else {
              this.checkPointType = 'aligned checkpoint';
            }
          } else if (this.checkPointDetail.checkpoint_type === 'SYNC_SAVEPOINT') {
            this.checkPointType = 'savepoint on cancel';
          } else if (this.checkPointDetail.checkpoint_type === 'SAVEPOINT') {
            this.checkPointType = 'savepoint';
          } else {
            this.checkPointType = '-';
          }
          this.isLoading = false;
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
    }
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobService.jobDetail$.pipe(first()).subscribe(data => {
      this.jobDetail = data;
      this.listOfVertex = data!.vertices;
      this.cdr.markForCheck();
      this.refresh();
    });
  }
}
