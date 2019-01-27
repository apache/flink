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

import { Component, Input, OnInit } from '@angular/core';
import { first } from 'rxjs/operators';
import { JobService } from 'flink-services';

@Component({
  selector   : 'flink-job-checkpoints-detail',
  templateUrl: './job-checkpoints-detail.component.html',
  styleUrls  : [ './job-checkpoints-detail.component.less' ]
})
export class JobCheckpointsDetailComponent implements OnInit {
  _checkPoint;
  @Input()
  set checkPoint(value) {
    this._checkPoint = value;
    this.refresh();
  }

  get checkPoint() {
    return this._checkPoint;
  }

  checkPointDetail;
  listOfVertex = [];
  isLoading = true;

  trackVertexBy(index, node) {
    return node.id;
  }

  refresh() {
    this.isLoading = true;
    if (this.jobService.jobDetail.jid) {
      this.jobService.loadCheckpointDetails(this.jobService.jobDetail.jid, this.checkPoint.id).subscribe(detail => {
        this.checkPointDetail = detail;
        this.isLoading = false;
      }, () => {
        this.isLoading = false;
      });
    }
  }

  constructor(private jobService: JobService) {
  }

  ngOnInit() {
    this.jobService.jobDetail$.pipe(
      first()
    ).subscribe(data => {
      this.listOfVertex = data.vertices;
      this.refresh();
    });
  }

}
