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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import {
  CheckPointConfigInterface,
  CheckPointHistoryInterface,
  CheckPointInterface,
  JobDetailCorrectInterface
} from 'interfaces';
import { distinctUntilChanged } from 'rxjs/operators';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-checkpoints',
  templateUrl: './job-checkpoints.component.html',
  styleUrls: ['./job-checkpoints.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobCheckpointsComponent implements OnInit {
  checkPointStats: CheckPointInterface;
  checkPointConfig: CheckPointConfigInterface;
  jobDetail: JobDetailCorrectInterface;

  trackHistoryBy(_: number, node: CheckPointHistoryInterface) {
    return node.id;
  }

  refresh() {
    this.jobService.loadCheckpointStats(this.jobDetail.jid).subscribe(data => (this.checkPointStats = data));
    this.jobService.loadCheckpointConfig(this.jobDetail.jid).subscribe(data => (this.checkPointConfig = data));
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobService.jobDetail$.pipe(distinctUntilChanged((pre, next) => pre.jid === next.jid)).subscribe(data => {
      this.jobDetail = data;
      this.jobService.loadCheckpointStats(this.jobDetail.jid).subscribe(stats => {
        this.checkPointStats = stats;
        this.cdr.markForCheck();
      });
      this.jobService.loadCheckpointConfig(this.jobDetail.jid).subscribe(config => {
        this.checkPointConfig = config;
        this.cdr.markForCheck();
      });
    });
  }
}
