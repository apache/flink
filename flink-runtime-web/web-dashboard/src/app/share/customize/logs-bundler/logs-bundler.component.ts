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

import {ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit} from '@angular/core';
import { switchMap, takeUntil} from "rxjs/operators";
import { Subject} from "rxjs";
import { StatusService} from "services";
import {LogsBundlerService} from "../../../services/logs-bundler.service";

@Component({
    selector: 'flink-logs-bundler',
    templateUrl: './logs-bundler.component.html',
    styleUrls: ['./logs-bundler.component.less'],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class LogsBundlerComponent implements OnInit, OnDestroy {
    private destroy$ = new Subject<void>();
    hideSpinner: boolean = true;
    message: string = ""
    hideDownloadButton: boolean = true;

    constructor(private logBundlerService: LogsBundlerService,
                private statusService: StatusService,
                private cdr: ChangeDetectorRef) {
    }

    ngOnInit() {
        this.statusService.refresh$.pipe(
            switchMap(() => this.logBundlerService.getStatus()),
            takeUntil(this.destroy$)
        ).subscribe( status => {
            this.message = status.message;
            this.hideSpinner = (status.status !== "PROCESSING");
            this.hideDownloadButton = (status.status !== "BUNDLE_READY");
            this.cdr.markForCheck();
        }, error => {
            this.message = "Error while fetching status: " + error.message;
            this.cdr.markForCheck();
        })
    }
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }

    requestArchive() {
        this.logBundlerService.triggerBundle().subscribe();
        this.hideSpinner = false;
        this.hideDownloadButton = true;
    }
    downloadArchive() {
        this.logBundlerService.download();
    }

}
