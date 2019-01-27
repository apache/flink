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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit, Optional } from '@angular/core';
import { Subject, Subscription } from 'rxjs';
import { ViewOperatorsDetail, ViewVerticesDetail } from 'flink-services';
import { RenderGroupNodeInfo, NzGraphComponent, RENDER_NODE_INFO, RENDER_NODE_INFO_CHANGE } from '@ng-zorro/ng-plus/graph';
import { JobOverviewGraphService } from '../../../../services/job-overview-graph.service';



@Component({
  selector: 'flink-vertices-node',
  templateUrl: './vertices-node.component.html',
  styleUrls: ['./vertices-node.component.less'],
  changeDetection    : ChangeDetectionStrategy.OnPush,
  preserveWhitespaces: false
})
export class VerticesNodeComponent implements OnInit, OnDestroy {

  change$ = Subscription.EMPTY;
  nodeselect$ = Subscription.EMPTY;
  verticesDetail: ViewVerticesDetail;
  operatorsDetail: ViewOperatorsDetail;
  canToggleExpand = true;

  constructor(
    @Optional() @Inject(RENDER_NODE_INFO) public nodeInfo: RenderGroupNodeInfo,
    @Optional() @Inject(RENDER_NODE_INFO_CHANGE) public nodeInfoChange: Subject<RenderGroupNodeInfo>,
    private graphComponent: NzGraphComponent,
    private cdRef: ChangeDetectorRef,
    private jobOverviewGraphService: JobOverviewGraphService
  ) {
    this.change$ = this.nodeInfoChange.asObservable().subscribe(_ => {
      this.update();
    });

    this.nodeselect$ = this.graphComponent.event.asObservable().subscribe(data => {
      if (data.eventName === 'node-select' && data.event.name === this.nodeInfo.node.name) {
        this.nodeClick();
      }
    });
  }

  ngOnInit() {
    this.update();
  }

  ngOnDestroy(): void {
    this.change$.unsubscribe();
    this.nodeselect$.unsubscribe();
  }

  update() {
    if (this.nodeInfo.node.isGroupNode) {
      this.verticesDetail = this.jobOverviewGraphService.getVerticesDetail(this.nodeInfo);
      this.canToggleExpand = this.jobOverviewGraphService.canToggleExpand(this.nodeInfo);
    } else {
      this.operatorsDetail = this.jobOverviewGraphService.getOperatorsDetail(this.nodeInfo);
    }
    this.cdRef.detectChanges();
  }

  nodeClick() {
    if (!this.nodeInfo.node.isGroupNode) {
      return;
    }
    this.graphComponent.event.emit({
      eventName: 'vertices-click',
      event: this.jobOverviewGraphService.getNodesItemCorrect(this.nodeInfo.node.name)
    });
  }

  toggleExpand($event: MouseEvent) {
    $event.preventDefault();
    $event.stopPropagation();
    this.graphComponent.nodeToggleExpand({
      name: this.nodeInfo.node.name
    });
  }

}
