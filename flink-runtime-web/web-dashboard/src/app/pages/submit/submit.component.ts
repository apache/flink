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

import { HttpEventType } from '@angular/common/http';
import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { Router } from '@angular/router';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { JarService, StatusService } from 'flink-services';
import { DagreComponent } from 'flink-share/common/dagre/dagre.component';

@Component({
  selector   : 'flink-submit',
  templateUrl: './submit.component.html',
  styleUrls  : [ './submit.component.less' ]
})
export class SubmitComponent implements OnInit, OnDestroy {
  @ViewChild(DagreComponent) dagreComponent: DagreComponent;
  expandedMap = {};
  isLoading = true;
  destroy$ = new Subject();
  listOfJar = [];
  address: string;
  isYarn = false;
  noaccess: string;
  isUploading = false;
  progress = 0;
  validateForm: FormGroup;
  visible = false;

  close() {
    this.visible = false;
  }


  uploadJar(file) {
    this.jarService.uploadJar(file).subscribe(event => {
      if (event.type === HttpEventType.UploadProgress) {
        this.isUploading = true;
        this.progress = Math.round(100 * event.loaded / event.total);
      } else if (event.type === HttpEventType.Response) {
        this.isUploading = false;
        this.statusService.manualRefresh();
      }
    }, () => {
      this.isUploading = false;
      this.progress = 0;
    });
  }

  deleteJar(jar) {
    this.jarService.deleteJar(jar.id).subscribe(() => {
      this.statusService.manualRefresh();
      this.expandedMap[ jar.id ] = false;
    });
  }

  expandJar(jar) {
    if (this.expandedMap[ jar.id ]) {
      this.expandedMap[ jar.id ] = false;
    } else {
      for (const key in this.expandedMap) {
        this.expandedMap[ key ] = false;
      }
      this.expandedMap[ jar.id ] = true;
    }
    if (jar.entry && jar.entry[ 0 ] && jar.entry[ 0 ].name) {
      this.validateForm.get('entryClass').setValue(jar.entry[ 0 ].name);
    } else{
      this.validateForm.get('entryClass').setValue(null);
    }
  }

  showPlan(jar) {
    this.jarService.getPlan(
      jar.id,
      this.validateForm.get('entryClass').value,
      this.validateForm.get('parallelism').value,
      this.validateForm.get('programArgs').value).subscribe(data => {
      const nodes = data.plan.nodes;
      const links = [];
      nodes.forEach(node => {
        if (node.inputs && node.inputs.length) {
          node.inputs.forEach(input => {
            links.push({ ...input, source: input.id, target: node.id, id: `${input.id}-${node.id}` });
          });
        }
      });
      this.visible = true;
      this.dagreComponent.flush(nodes, links, true);
    });
  }

  submit(jar) {
    this.jarService.runJob(
      jar.id,
      this.validateForm.get('entryClass').value,
      this.validateForm.get('parallelism').value,
      this.validateForm.get('programArgs').value,
      this.validateForm.get('savepointPath').value,
      this.validateForm.get('allowNonRestoredState').value
    ).subscribe(data => {
      this.router.navigate([ 'job', data.jobid ]).then();
    });
  }

  trackJarBy(index, node) {
    return node.id;
  }

  constructor(private jarService: JarService, private statusService: StatusService, private fb: FormBuilder, private router: Router) {
  }

  ngOnInit() {
    this.isYarn = window.location.href.indexOf('/proxy/application_') !== -1;
    this.validateForm = this.fb.group({
      entryClass           : [ null ],
      parallelism          : [ null ],
      programArgs          : [ null ],
      savepointPath        : [ null ],
      allowNonRestoredState: [ null ]
    });
    this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      flatMap(() => this.jarService.loadJarList())
    ).subscribe(data => {
      this.isLoading = false;
      this.listOfJar = data.files;
      this.address = data.address;
    }, () => {
      this.isLoading = false;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}
