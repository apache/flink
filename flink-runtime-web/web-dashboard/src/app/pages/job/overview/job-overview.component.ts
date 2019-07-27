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

import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  OnDestroy,
  OnInit,
  ViewChild
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { LONG_MIN_VALUE } from 'config';
import { forkJoin, Observable, of, Subject } from 'rxjs';
import { catchError, filter, map, takeUntil } from 'rxjs/operators';
import { NodesItemCorrectInterface, NodesItemLinkInterface } from 'interfaces';
import { JobService, MetricsService } from 'services';
import { DagreComponent } from 'share/common/dagre/dagre.component';

@Component({
  selector: 'flink-job-overview',
  templateUrl: './job-overview.component.html',
  styleUrls: ['./job-overview.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewComponent implements OnInit, OnDestroy {
  @ViewChild(DagreComponent) dagreComponent: DagreComponent;
  nodes: NodesItemCorrectInterface[] = [];
  links: NodesItemLinkInterface[] = [];
  destroy$ = new Subject();
  selectedNode: NodesItemCorrectInterface | null;
  top = 500;
  jobId: string;
  timeoutId: number;

  onNodeClick(node: NodesItemCorrectInterface) {
    if (!(this.selectedNode && this.selectedNode.id === node.id)) {
      this.router.navigate([node.id], { relativeTo: this.activatedRoute }).then();
    }
  }

  onResizeEnd() {
    if (!this.selectedNode) {
      this.dagreComponent.moveToCenter();
    } else {
      this.dagreComponent.focusNode(this.selectedNode, true);
    }
  }

  mergeWithWatermarks(nodes: NodesItemCorrectInterface[]): Observable<NodesItemCorrectInterface[]> {
    return forkJoin(
      nodes.map(node => {
        const listOfMetricId = [];
        let lowWatermark = NaN;
        for (let i = 0; i < node.parallelism; i++) {
          listOfMetricId.push(`${i}.currentInputWatermark`);
        }
        return this.metricService.getMetrics(this.jobId, node.id, listOfMetricId).pipe(
          map(metrics => {
            let minValue = NaN;
            const watermarks: { [index: string]: number } = {};
            for (const key in metrics.values) {
              const value = metrics.values[key];
              const subtaskIndex = key.replace('.currentInputWatermark', '');
              watermarks[subtaskIndex] = value;
              if (isNaN(minValue) || value < minValue) {
                minValue = value;
              }
            }
            if (!isNaN(minValue) && minValue > LONG_MIN_VALUE) {
              lowWatermark = minValue;
            } else {
              lowWatermark = NaN;
            }
            return { ...node, lowWatermark };
          })
        );
      })
    ).pipe(catchError(() => of(nodes)));
  }

  refreshNodesWithWatermarks() {
    this.mergeWithWatermarks(this.nodes).subscribe(nodes => {
      nodes.forEach(node => {
        this.dagreComponent.updateNode(node.id, node);
      });
    });
  }

  constructor(
    private jobService: JobService,
    private router: Router,
    private activatedRoute: ActivatedRoute,
    public elementRef: ElementRef,
    private metricService: MetricsService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit() {
    this.jobService.jobDetail$
      .pipe(
        filter(job => job.jid === this.activatedRoute.parent!.parent!.snapshot.params.jid),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        if (this.jobId !== data.plan.jid) {
          this.nodes = data.plan.nodes;
          this.links = data.plan.links;
          this.jobId = data.plan.jid;
          this.dagreComponent.flush(this.nodes, this.links, true).then();
          this.refreshNodesWithWatermarks();
        } else {
          this.nodes = data.plan.nodes;
          this.refreshNodesWithWatermarks();
        }
        this.cdr.markForCheck();
      });

    this.jobService.selectedVertex$.pipe(takeUntil(this.destroy$)).subscribe(data => {
      if (data) {
        this.dagreComponent.focusNode(data);
      } else if (this.selectedNode) {
        this.timeoutId = setTimeout(() => this.dagreComponent.redrawGraph());
      }
      this.selectedNode = data;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    clearTimeout(this.timeoutId);
  }
}
