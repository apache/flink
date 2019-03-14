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
import { JobManagerService } from 'services';

@Component({
  selector: 'flink-job-manager-configuration',
  templateUrl: './job-manager-configuration.component.html',
  styleUrls: ['./job-manager-configuration.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobManagerConfigurationComponent implements OnInit {
  listOfConfig: Array<{ key: string; value: string }> = [];

  constructor(private jobManagerService: JobManagerService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobManagerService.loadConfig().subscribe(data => {
      this.listOfConfig = data;
      this.cdr.markForCheck();
    });
  }
}
