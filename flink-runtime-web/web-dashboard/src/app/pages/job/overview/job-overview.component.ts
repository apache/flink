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

import { NgIf } from '@angular/common';
import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  OnDestroy,
  OnInit,
  ViewChild
} from '@angular/core';
import { ActivatedRoute, Router, RouterOutlet } from '@angular/router';
import { forkJoin, Observable, of, Subject } from 'rxjs';
import { catchError, filter, map, mergeMap, takeUntil } from 'rxjs/operators';

import { DagreComponent } from '@flink-runtime-web/components/dagre/dagre.component';
import { ResizeComponent } from '@flink-runtime-web/components/resize/resize.component';
import { NodesItemCorrect, NodesItemLink } from '@flink-runtime-web/interfaces';
import { JobOverviewListComponent } from '@flink-runtime-web/pages/job/overview/list/job-overview-list.component';
import { JobService, MetricsService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzNotificationService } from 'ng-zorro-antd/notification';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-job-overview',
  templateUrl: './job-overview.component.html',
  styleUrls: ['./job-overview.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzAlertModule, NgIf, DagreComponent, RouterOutlet, JobOverviewListComponent, ResizeComponent],
  standalone: true
})
export class JobOverviewComponent implements OnInit, OnDestroy {
  public nodes: NodesItemCorrect[] = [];
  public links: NodesItemLink[] = [];
  public selectedNode: NodesItemCorrect | null;
  public top = 500;
  public jobId: string;
  public timeoutId: number;

  @ViewChild(DagreComponent, { static: true }) private readonly dagreComponent: DagreComponent;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly router: Router,
    private readonly activatedRoute: ActivatedRoute,
    public readonly elementRef: ElementRef,
    private readonly metricService: MetricsService,
    private readonly jobLocalService: JobLocalService,
    private readonly jobService: JobService,
    private readonly notificationService: NzNotificationService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        filter(job => job.jid === this.activatedRoute.parent!.parent!.snapshot.params.jid),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        if (this.jobId !== data.plan.jid || this.nodes.length === 0) {
          this.nodes = data.plan.nodes;
          this.links = data.plan.links;
          this.jobId = data.plan.jid;
          this.dagreComponent.flush(this.nodes, this.links, true).then();
          this.refreshNodesWithMetrics();
        } else {
          this.nodes = data.plan.nodes;
          this.refreshNodesWithMetrics();
        }
        this.cdr.markForCheck();
      });

    this.jobLocalService
      .selectedVertexChanges()
      .pipe(takeUntil(this.destroy$))
      .subscribe(data => {
        if (data) {
          this.dagreComponent.focusNode(data);
        } else if (this.selectedNode) {
          this.timeoutId = window.setTimeout(() => this.dagreComponent.redrawGraph());
        }
        this.selectedNode = data;
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    clearTimeout(this.timeoutId);
  }

  public onNodeClick(node: NodesItemCorrect): void {
    if (!(this.selectedNode && this.selectedNode.id === node.id)) {
      this.router.navigate([node.id], { relativeTo: this.activatedRoute }).then();
    }
  }

  public onRescale(desiredParallelism: Map<string, number>): void {
    this.jobService.changeDesiredParallelism(this.jobId, desiredParallelism).subscribe(() => {
      this.notificationService.success(
        'Rescaling operation.',
        'Job resources requirements have been updated. Job will now try to rescale.'
      );
    });
  }

  public onResizeEnd(): void {
    if (!this.selectedNode) {
      this.dagreComponent.moveToCenter();
    } else {
      this.dagreComponent.focusNode(this.selectedNode, true);
    }
    this.cdr.markForCheck();
  }

  public refreshNodesWithMetrics(): void {
    this.mergeWithBackPressure(this.nodes)
      .pipe(
        mergeMap(nodes => this.mergeWithWatermarks(nodes)),
        takeUntil(this.destroy$)
      )
      .subscribe(nodes => {
        nodes.forEach(node => {
          this.dagreComponent.updateNode(node.id, node);
          this.cdr.markForCheck();
        });
      });
  }

  private mergeWithBackPressure(nodes: NodesItemCorrect[]): Observable<NodesItemCorrect[]> {
    return forkJoin(
      nodes.map(node => {
        return this.metricService
          .loadAggregatedMetrics(this.jobId, node.id, ['backPressuredTimeMsPerSecond', 'busyTimeMsPerSecond'])
          .pipe(
            map(result => {
              return {
                ...node,
                backPressuredPercentage: Math.min(Math.round(result.backPressuredTimeMsPerSecond / 10), 100),
                busyPercentage: Math.min(Math.round(result.busyTimeMsPerSecond / 10), 100)
              };
            })
          );
      })
    ).pipe(catchError(() => of(nodes)));
  }

  private mergeWithWatermarks(nodes: NodesItemCorrect[]): Observable<NodesItemCorrect[]> {
    return forkJoin(
      nodes.map(node => {
        return this.metricService.loadWatermarks(this.jobId, node.id).pipe(
          map(result => {
            return { ...node, lowWatermark: result.lowWatermark };
          })
        );
      })
    ).pipe(catchError(() => of(nodes)));
  }
}
