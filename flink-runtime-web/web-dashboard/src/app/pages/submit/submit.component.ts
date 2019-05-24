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
import { ArtifactFilesItemInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { ArtifactService, StatusService } from 'services';
import { DagreComponent } from 'share/common/dagre/dagre.component';

@Component({
  selector: 'flink-submit',
  templateUrl: './submit.component.html',
  styleUrls: ['./submit.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class SubmitComponent implements OnInit, OnDestroy {
  @ViewChild(DagreComponent) dagreComponent: DagreComponent;
  expandedMap = new Map();
  isLoading = true;
  destroy$ = new Subject();
  listOfArtifact: ArtifactFilesItemInterface[] = [];
  address: string;
  isYarn = false;
  noAccess = false;
  isUploading = false;
  progress = 0;
  validateForm: FormGroup;
  planVisible = false;

  /**
   * Upload artifact
   * @param file
   */
  uploadArtifact(file: File) {
    this.artifactService.uploadArtifact(file).subscribe(
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
   * Delete artifact
   * @param artifact
   */
  deleteArtifact(artifact: ArtifactFilesItemInterface) {
    this.artifactService.deleteArtifact(artifact.id).subscribe(() => {
      this.statusService.forceRefresh();
      this.expandedMap.set(artifact.id, false);
    });
  }

  /**
   * Click to expand artifact details
   * @param artifact
   */
  expandArtifact(artifact: ArtifactFilesItemInterface) {
    if (this.expandedMap.get(artifact.id)) {
      this.expandedMap.set(artifact.id, false);
    } else {
      this.expandedMap.forEach((_, key) => {
        this.expandedMap.set(key, false);
        this.validateForm.reset();
      });
      this.expandedMap.set(artifact.id, true);
    }
    if (artifact.entry && artifact.entry[0] && artifact.entry[0].name) {
      this.validateForm.get('entryClass')!.setValue(artifact.entry[0].name);
    } else {
      this.validateForm.get('entryClass')!.setValue(null);
    }
  }

  /**
   * Show Plan Visualization
   * @param artifact
   */
  showPlan(artifact: ArtifactFilesItemInterface) {
    this.artifactService
      .getPlan(
        artifact.id,
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
   * @param artifact
   */
  submitJob(artifact: ArtifactFilesItemInterface) {
    this.artifactService
      .runJob(
        artifact.id,
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
  trackArtifactBy(_: number, node: ArtifactFilesItemInterface) {
    return node.id;
  }

  constructor(
    private artifactService: ArtifactService,
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
        flatMap(() => this.artifactService.loadArtifactList())
      )
      .subscribe(
        data => {
          this.isLoading = false;
          this.listOfArtifact = data.files;
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
