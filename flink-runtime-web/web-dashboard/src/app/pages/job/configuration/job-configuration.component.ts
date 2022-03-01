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
import { mergeMap } from 'rxjs/operators';

import { JobConfig } from 'interfaces';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-configuration',
  templateUrl: './job-configuration.component.html',
  styleUrls: ['./job-configuration.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobConfigurationComponent implements OnInit {
  public config: JobConfig;
  public listOfUserConfig: Array<{ key: string; value: string }> = [];

  constructor(private readonly jobService: JobService, private readonly cdr: ChangeDetectorRef) {}

  public ngOnInit(): void {
    this.jobService.jobDetail$.pipe(mergeMap(job => this.jobService.loadJobConfig(job.jid))).subscribe(data => {
      this.config = data;
      const userConfig = this.config['execution-config']['user-config'];
      const array = [];
      for (const key in userConfig) {
        array.push({
          key,
          value: userConfig[key]
        });
      }
      this.listOfUserConfig = array.sort((pre, next) => (pre.key > next.key ? 1 : -1));
      this.cdr.markForCheck();
    });
  }
}
