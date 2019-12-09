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
import * as d3 from 'd3';

@Component({
  selector: 'flink-svg-container',
  templateUrl: './svg-container.component.html',
  styleUrls: ['./svg-container.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class SvgContainerComponent implements OnInit, AfterContentInit {
  zoom = 1;
  width: number;
  height: number;
  transform = 'translate(0, 0) scale(1)';
  containerTransform = { x: 0, y: 0, k: 1 };
  svgSelect: Selection<any, any, any, any>;
  zoomController: ZoomBehavior<any, any>;
  @ViewChild('svgContainer') svgContainer: ElementRef<SVGAElement>;
  @ViewChild('svgInner') svgInner: ElementRef<SVGAElement>;
  @Input() nzMaxZoom = 5;
  @Input() nzMinZoom = 0.1;
  @Output() clickBgEvent: EventEmitter<MouseEvent> = new EventEmitter();
  @Output() zoomEvent: EventEmitter<number> = new EventEmitter();
  @Output() transformEvent: EventEmitter<{ x: number; y: number; scale: number }> = new EventEmitter();

  /**
   * Zoom to spec level
   * @param zoomLevel
   */
  zoomTo(zoomLevel: number) {
    this.svgSelect
      .transition()
      .duration(0)
      .call(this.zoomController.scaleTo, zoomLevel);
  }

  /**
   * Set transform position
   * @param transform
   * @param animate
   */
  setPositionByTransform(transform: { x: number; y: number; k: number }, animate = false) {
    this.svgSelect
      .transition()
      .duration(animate ? 500 : 0)
      .call(this.zoomController.transform, transform);
  }

  constructor(private el: ElementRef) {}

  ngOnInit() {
    this.svgSelect = select(this.svgContainer.nativeElement);
    this.zoomController = zoom()
      .scaleExtent([this.nzMinZoom, this.nzMaxZoom])
      .on('zoom', () => {
        this.containerTransform = d3.event.transform;
        this.zoom = this.containerTransform.k;
        if (!isNaN(this.containerTransform.x)) {
          this.transform = `translate(${this.containerTransform.x} ,${this.containerTransform.y})scale(${
            this.containerTransform.k
          })`;
        }
        this.zoomEvent.emit(this.zoom);
        this.transformEvent.emit(this.containerTransform as any);
      });
    this.svgSelect.call(this.zoomController).on('wheel.zoom', null);
  }

  ngAfterContentInit() {
    const hostElem = this.el.nativeElement;
    if (hostElem.parentNode !== null) {
      const dims = hostElem.parentNode.getBoundingClientRect();
      this.width = dims.width;
      this.height = dims.height;
      this.zoomTo(this.zoom);
    }
  }
}
