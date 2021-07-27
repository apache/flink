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

import { HttpEventType } from '@angular/common/http';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { Router } from '@angular/router';
import { JarFilesItemInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { JarService, StatusService } from 'services';
import { DagreComponent } from 'share/common/dagre/dagre.component';

@Component({
  selector: 'flink-submit',
  templateUrl: './submit.component.html',
  styleUrls: ['./submit.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class SubmitComponent implements OnInit, OnDestroy {
  @ViewChild(DagreComponent, { static: true }) dagreComponent: DagreComponent;
  expandedMap = new Map();
  isLoading = true;
  destroy$ = new Subject();
  listOfJar: JarFilesItemInterface[] = [];
  address: string;
  isYarn = false;
  noAccess = false;
  isUploading = false;
  progress = 0;
  validateForm: FormGroup;
  planVisible = false;

  /**
   * Upload jar
   * @param file
   */
  uploadJar(file: File) {
    this.jarService.uploadJar(file).subscribe(
      event => {
        if (event.type === HttpEventType.UploadProgress && event.total) {
          this.isUploading = true;
          this.progress = Math.round((100 * event.loaded) / event.total);
        } else if (event.type === HttpEventType.Response) {
          this.isUploading = false;
          this.statusService.forceRefresh();
        }
      },
      () => {
        this.isUploading = false;
        this.progress = 0;
      }
    );
  }

  /**
   * Delete jar
   * @param jar
   */
  deleteJar(jar: JarFilesItemInterface) {
    this.jarService.deleteJar(jar.id).subscribe(() => {
      this.statusService.forceRefresh();
      this.expandedMap.set(jar.id, false);
    });
  }

  /**
   * Click to expand jar details
   * @param jar
   */
  expandJar(jar: JarFilesItemInterface) {
    if (this.expandedMap.get(jar.id)) {
      this.expandedMap.set(jar.id, false);
    } else {
      this.expandedMap.forEach((_, key) => {
        this.expandedMap.set(key, false);
        this.validateForm.reset();
      });
      this.expandedMap.set(jar.id, true);
    }
    if (jar.entry && jar.entry[0] && jar.entry[0].name) {
      this.validateForm.get('entryClass')!.setValue(jar.entry[0].name);
    } else {
      this.validateForm.get('entryClass')!.setValue(null);
    }
  }

  /**
   * Show Plan Visualization
   * @param jar
   */
  showPlan(jar: JarFilesItemInterface) {
    this.jarService
      .getPlan(
        jar.id,
        this.validateForm.get('entryClass')!.value,
        this.validateForm.get('parallelism')!.value,
        this.validateForm.get('programArgs')!.value
      )
      .subscribe(data => {
        this.planVisible = true;
        this.dagreComponent.flush(data.nodes, data.links, true);
      });
  }

  /**
   * Close Plan Visualization
   */
  hidePlan() {
    this.planVisible = false;
  }

  /**
   * Submit job
   * @param jar
   */
  submitJob(jar: JarFilesItemInterface) {
    this.jarService
      .runJob(
        jar.id,
        this.validateForm.get('entryClass')!.value,
        this.validateForm.get('parallelism')!.value,
        this.validateForm.get('programArgs')!.value,
        this.validateForm.get('savepointPath')!.value,
        this.validateForm.get('allowNonRestoredState')!.value
      )
      .subscribe(data => {
        this.router.navigate(['job', data.jobid]).then();
      });
  }

  /**
   * trackBy Func
   * @param _
   * @param node
   */
  trackJarBy(_: number, node: JarFilesItemInterface) {
    return node.id;
  }

  constructor(
    private jarService: JarService,
    private statusService: StatusService,
    private fb: FormBuilder,
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit() {
    this.isYarn = window.location.href.indexOf('/proxy/application_') !== -1;
    this.validateForm = this.fb.group({
      entryClass: [null],
      parallelism: [null],
      programArgs: [null],
      savepointPath: [null],
      allowNonRestoredState: [null]
    });
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() => this.jarService.loadJarList())
      )
      .subscribe(
        data => {
          this.isLoading = false;
          this.listOfJar = data.files;
          this.address = data.address;
          this.cdr.markForCheck();
          this.noAccess = Boolean(data.error);
        },
        () => {
          this.isLoading = false;
          this.noAccess = true;
          this.cdr.markForCheck();
        }
      );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
