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

import { NzGraphComponent } from '@ng-zorro/ng-plus/graph';
import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  Inject,
  OnDestroy,
  OnInit,
  Optional,
  ViewChild
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NodesItemCorrectInterface } from 'flink-interfaces';
import { JobOverviewGraphService, JobService } from 'flink-services';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { JOB_OVERVIEW_CONFIG, JobOverviewConfig } from './job-overview.config';

@Component({
  selector       : 'flink-job-overview',
  templateUrl    : './job-overview.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-overview.component.less' ]
})
export class JobOverviewComponent implements OnInit, OnDestroy {
  nodes = [];
  links = [];
  destroy$ = new Subject();
  top = 500;
  selectedNode = null;
  verticesNodeComponent;
  jobId: string;
  canToggleExpand = true;
  @ViewChild(NzGraphComponent) graphComponent: NzGraphComponent;

  onNodeClick(node: NodesItemCorrectInterface) {
    if (this.selectedNode && node.id === this.selectedNode.id) {
      return;
    }
    this.jobOverviewGraphService.setTransformCache();
    this.router.navigate([ node.id ], { relativeTo: this.activatedRoute }).then(() => {
      this.panToCenterByNodeName(node.id);
    });
  }

  onListNodeClick(node: NodesItemCorrectInterface) {
    this.graphComponent.fire('node-select', {
      name: node.id
    });
    this.onNodeClick(node);
  }

  onCloseDrawer() {
    setTimeout(() => {
      this.jobOverviewGraphService.resetTransform(this.graphComponent);
    }, 200);
  }

  panToCenterByNodeName(name: string) {
    setTimeout(() => {
      this.graphComponent.panToCenterByNodeName(name);
    }, 500);
  }

  constructor(
    private jobService: JobService,
    public jobOverviewGraphService: JobOverviewGraphService,
    public elementRef: ElementRef,
    private router: Router,
    private cdr: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute,
    @Optional() @Inject(JOB_OVERVIEW_CONFIG) config: JobOverviewConfig
  ) {
    this.verticesNodeComponent = config.verticesNodeComponent;
  }

  ngOnInit() {
    this.jobService.jobDetailLatest$.pipe(
      takeUntil(this.destroy$),
    ).subscribe(data => {
      if (!data || !data.plan || !data.verticesDetail) {
        return;
      }
      this.canToggleExpand  = data.verticesDetail.operators && data.verticesDetail.operators.length > 0;
      data.verticesDetail = this.jobService.fillEmptyOperators(data.plan.nodes, data.verticesDetail);
      if (data.plan.jid !== this.jobId) {
        this.jobId = data.plan.jid;
        this.selectedNode = null;
        this.nodes = data.plan.nodes;
        this.links = data.plan.links;
        this.jobOverviewGraphService.initGraph(this.graphComponent, data);
        this.cdr.markForCheck();
      } else {
        this.nodes = data.plan.nodes;
        this.jobOverviewGraphService.updateData(data);
      }
    });
    this.jobService.selectedVertexNode$.pipe(takeUntil(this.destroy$)).subscribe((data) => {
      this.selectedNode = data;
      if (!this.selectedNode) {
        this.onCloseDrawer();
      }
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.jobService.selectedVertexNode$.next(null);
  }

  dagreEvent($event) {
    switch ($event.eventName) {
      case 'vertices-click':
        this.onNodeClick($event.event);
        break;
      default:
        break;
    }
  }

  collapseAll() {
    this.graphComponent.expandOrCollapseAll(false);
    setTimeout(() => {
      this.graphComponent.fit();
    }, 300);
  }

  expandAll() {
    this.graphComponent.expandOrCollapseAll(true);
    setTimeout(() => {
      this.graphComponent.fit();
      this.graphComponent.traceInputs();
    }, 300);
  }
}
