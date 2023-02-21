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

import { DatePipe, NgForOf, NgIf } from '@angular/common';
import { HttpEventType } from '@angular/common/http';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit, ViewChild } from '@angular/core';
import { ReactiveFormsModule, UntypedFormBuilder, UntypedFormGroup } from '@angular/forms';
import { Router } from '@angular/router';
import { Subject } from 'rxjs';
import { mergeMap, takeUntil } from 'rxjs/operators';

import { DagreComponent } from '@flink-runtime-web/components/dagre/dagre.component';
import { FileReadDirective } from '@flink-runtime-web/components/file-read.directive';
import { JarFilesItem } from '@flink-runtime-web/interfaces';
import { JarService, StatusService } from '@flink-runtime-web/services';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTableModule } from 'ng-zorro-antd/table';

@Component({
  selector: 'flink-submit',
  templateUrl: './submit.component.html',
  styleUrls: ['./submit.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzCardModule,
    NzTableModule,
    NgIf,
    NgForOf,
    DatePipe,
    NzPopconfirmModule,
    NzFormModule,
    NzInputModule,
    ReactiveFormsModule,
    NzIconModule,
    NzCheckboxModule,
    NzButtonModule,
    FileReadDirective,
    NzProgressModule,
    NzDrawerModule,
    DagreComponent
  ],
  standalone: true
})
export class SubmitComponent implements OnInit, OnDestroy {
  public readonly trackById = (_: number, node: JarFilesItem): string => node.id;

  public expandedMap = new Map<string, boolean>();
  public isLoading = true;
  public listOfJar: JarFilesItem[] = [];
  public address: string;
  public isYarn = false;
  public noAccess = false;
  public isUploading = false;
  public progress = 0;
  public validateForm: UntypedFormGroup;
  public planVisible = false;

  @ViewChild(DagreComponent, { static: true }) private readonly dagreComponent: DagreComponent;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jarService: JarService,
    private readonly statusService: StatusService,
    private readonly fb: UntypedFormBuilder,
    private readonly router: Router,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
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
        mergeMap(() => this.jarService.loadJarList())
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

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public uploadJar(file: File): void {
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

  public deleteJar(jar: JarFilesItem): void {
    this.jarService.deleteJar(jar.id).subscribe(() => {
      this.statusService.forceRefresh();
      this.expandedMap.set(jar.id, false);
    });
  }

  public expandJar(jar: JarFilesItem): void {
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

  public showPlan(jar: JarFilesItem): void {
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

  public hidePlan(): void {
    this.planVisible = false;
  }

  public submitJob(jar: JarFilesItem): void {
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
        this.router.navigate(['job', 'running', data.jobid]).then();
      });
  }
}
