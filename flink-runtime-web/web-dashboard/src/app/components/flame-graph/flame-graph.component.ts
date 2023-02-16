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

import { Component, ChangeDetectionStrategy, ElementRef, Input, ViewChild } from '@angular/core';

import { FlameGraphType, JobFlameGraphNode } from '@flink-runtime-web/interfaces';
import * as _d3 from 'd3';
import { flamegraph, offCpuColorMapper } from 'd3-flame-graph';
import { format } from 'd3-format';
import { select } from 'd3-selection';
import _d3Tip from 'd3-tip';

@Component({
  selector: 'flink-flame-graph',
  templateUrl: './flame-graph.component.html',
  styleUrls: [],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: true
})
export class FlameGraphComponent {
  @ViewChild('flameGraphContainer', { static: true }) flameGraphContainer: ElementRef<Element>;
  @Input() data: JobFlameGraphNode;
  @Input() graphType: FlameGraphType;

  draw(): void {
    if (this.data) {
      const element = this.flameGraphContainer.nativeElement;
      const chart = flamegraph().width(element.clientWidth);

      const d3 = { ..._d3, tip: _d3Tip };

      const tip = d3
        .tip()
        .direction('s')
        .offset([8, 0])
        .attr('class', 'd3-flame-graph-tip')
        .html(function (d: { data: { name: string; value: string }; x1: number; x0: number }) {
          return `${d.data.name} (${format('.3f')(100 * (d.x1 - d.x0))}%, ${d.data.value} samples)`;
        });

      chart.tooltip(tip);

      if (this.graphType == FlameGraphType.OFF_CPU) {
        chart.setColorMapper(offCpuColorMapper);
      }

      select(element).selectAll('*').remove();
      select(element).datum(this.data).call(chart);
    }
  }
}
