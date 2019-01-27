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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subject } from 'rxjs';
import { filter, flatMap, takeUntil, tap } from 'rxjs/operators';
import { JobService, StatusService } from 'flink-services';

@Component({
  selector   : 'flink-job',
  templateUrl: './job.component.html',
  styleUrls  : [ './job.component.less' ]
})
export class JobComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  isLoading = true;
  refreshing = false;

  constructor(private activatedRoute: ActivatedRoute, private jobService: JobService, private statusService: StatusService) {
  }

  ngOnInit() {
    Promise.resolve().then(() => this.statusService.isCollapsed = true);
    this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      filter(() => !this.refreshing),
      flatMap(() => {
        this.refreshing = true;
        return this.jobService.loadJobWithVerticesDetail(this.activatedRoute.snapshot.params.jid);
      })
    ).subscribe(data => {
      this.jobService.setJobDetail(data);
      this.isLoading = false;
      this.refreshing = false;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.jobService.jobDetail = null;
  }

}
