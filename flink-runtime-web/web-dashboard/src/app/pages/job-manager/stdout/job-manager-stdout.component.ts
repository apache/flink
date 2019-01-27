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

import { ChangeDetectorRef, Component, OnInit, ViewChild, ChangeDetectionStrategy } from '@angular/core';
import { ConfigService, JobManagerService } from 'flink-services';
import { NzMonacoEditorComponent } from '@ng-zorro/ng-plus';

@Component({
  selector       : 'flink-job-manager-stdout',
  templateUrl    : './job-manager-stdout.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-manager-stdout.component.less' ]
})
export class JobManagerStdoutComponent implements OnInit {
  @ViewChild(NzMonacoEditorComponent) monacoEditorComponent: NzMonacoEditorComponent;
  stdout = '';
  pageChanged(page) {
    this.jobManagerService.loadStdout(page).subscribe(data => {
      this.stdout = data;
      this.cdr.markForCheck();
    });
  }
  constructor(public configService: ConfigService, private jobManagerService: JobManagerService, private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    this.jobManagerService.loadStdout().subscribe(data => {
      // @ts-ignore
      this.monacoEditorComponent.layout();
      this.stdout = data;
      this.cdr.markForCheck();
    });
  }
}
