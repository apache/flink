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
  AfterContentInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';

import { select, Selection } from 'd3-selection';
import { zoom, ZoomBehavior } from 'd3-zoom';

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
type SafeAny = any;

@Component({
  selector: 'flink-svg-container',
  templateUrl: './svg-container.component.html',
  styleUrls: ['./svg-container.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: true
})
export class SvgContainerComponent implements OnInit, AfterContentInit {
  public zoom = 1;
  public width: number;
  public height: number;
  public transform = 'translate(0, 0) scale(1)';
  public containerTransform = { x: 0, y: 0, k: 1 };
  public svgSelect: Selection<SafeAny, SafeAny, SafeAny, SafeAny>;
  public zoomController: ZoomBehavior<SafeAny, SafeAny>;

  @Input() public nzMaxZoom = 5;
  @Input() public nzMinZoom = 0.1;

  @Output() public readonly clickBgEvent = new EventEmitter<MouseEvent>();
  @Output() public readonly zoomEvent = new EventEmitter<number>();
  @Output() public readonly transformEvent = new EventEmitter<{ x: number; y: number; scale: number }>();

  @ViewChild('svgContainer', { static: true }) private readonly svgContainer: ElementRef<SVGAElement>;

  public zoomTo(zoomLevel: number): void {
    this.svgSelect.transition().duration(0).call(this.zoomController.scaleTo, zoomLevel);
  }

  public setPositionByTransform(transform: { x: number; y: number; k: number }, animate = false): void {
    this.svgSelect
      .transition()
      .duration(animate ? 500 : 0)
      .call(this.zoomController.transform, transform);
  }

  constructor(private readonly el: ElementRef, private cdr: ChangeDetectorRef) {}

  public ngOnInit(): void {
    this.svgSelect = select(this.svgContainer.nativeElement);
    this.zoomController = zoom()
      .scaleExtent([this.nzMinZoom, this.nzMaxZoom])
      .on('zoom', ({ transform }: SafeAny) => {
        const { x, y, k } = transform;
        this.zoom = k;
        this.containerTransform = transform;
        if (!isNaN(x)) {
          this.transform = `translate(${x} ,${y})scale(${k})`;
        }
        this.zoomEvent.emit(k);
        this.transformEvent.emit(transform);
        this.cdr.markForCheck();
      });
    this.svgSelect.call(this.zoomController).on('wheel.zoom', null);
  }

  public ngAfterContentInit(): void {
    const hostElem = this.el.nativeElement;
    if (hostElem.parentNode !== null) {
      const dims = hostElem.parentNode.getBoundingClientRect();
      this.width = dims.width;
      this.height = dims.height;
      this.zoomTo(this.zoom);
    }
  }
}
