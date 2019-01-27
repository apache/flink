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

import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilKeyChanged, takeUntil } from 'rxjs/operators';
import { JobDetailCorrectInterface } from 'flink-interfaces';
import { JobService } from 'flink-services';

@Component({
  selector   : 'flink-job-status',
  templateUrl: './job-status.component.html',
  styleUrls  : [ './job-status.component.less' ]
})
export class JobStatusComponent implements OnInit, OnDestroy {
  @Input() isLoading = true;
  tips;
  destroy$ = new Subject();
  listOfNavigation = [
    {
      pathOrParam: 'overview',
      title      : 'Overview'
    },
    {
      pathOrParam: 'exceptions',
      title      : 'Exceptions'
    },
    {
      pathOrParam: 'timeline',
      title      : 'TimeLine'
    },
    {
      pathOrParam: 'checkpoints',
      title      : 'Checkpoints'
    },
    {
      pathOrParam: 'configuration',
      title      : 'Configuration'
    },
    {
      pathOrParam: 'pending-slots',
      title      : 'Pending Slots'
    }
  ];

  get detail(): JobDetailCorrectInterface {
    return this.jobService.jobDetail;
  }

  stopJob() {
    this.jobService.stopJob(this.detail.jid).subscribe(() => {
      this.tips = 'Stopping...';
    });
  }

  cancelJob() {
    this.jobService.cancelJob(this.detail.jid).subscribe(() => {
      this.tips = 'Cancelling...';
    });
  }

  constructor(private jobService: JobService) {
  }

  ngOnInit() {
    this.jobService.jobDetail$.pipe(
      takeUntil(this.destroy$),
      distinctUntilKeyChanged('state')
    ).subscribe(() => {
      this.tips = '';
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}
