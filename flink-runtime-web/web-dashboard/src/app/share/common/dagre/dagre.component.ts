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

import {
  AfterContentInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  NgZone,
  OnChanges,
  Output,
  QueryList,
  SimpleChanges,
  ViewChild,
  ViewChildren
} from '@angular/core';

import { select } from 'd3';
import { zoomIdentity } from 'd3-zoom';
import { NodesItemCorrectInterface } from 'flink-interfaces';
import { LayoutNode, NodeShape, NzGraph } from './graph';
import { NodeRectComponent } from './node-rect.component';
import { NodeComponent } from './node.component';
import { SvgContainerComponent } from './svg-container.component';

enum Visibility {
  Hidden  = 'hidden',
  Visible = 'visible'
}

@Component({
  selector       : 'flink-dagre',
  templateUrl    : './dagre.component.html',
  styleUrls      : [ './dagre.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class DagreComponent extends NzGraph implements OnChanges, AfterContentInit {
  visibility: Visibility | string = Visibility.Hidden;
  circleNodeIds: string[] = [];
  focusedLinkIds: string[] = [];
  selectedNodeId: string;
  panEnable = true;
  zoom = 1;
  cacheTransform = { x: 0, y: 0, k: 1 };
  oldTransform = { x: 0, y: 0, k: 1 };
  cacheNodes = [];
  cacheLinks = [];
  @Input() xCenter = 2;
  @Input() yCenter = 2;
  @Input() nodeShape: NodeShape = NodeShape.Rect;
  @Output() nodeClick = new EventEmitter();
  @ViewChildren('nodeElement') nodeElements: QueryList<ElementRef>;
  @ViewChildren('linkElement') linkElements: QueryList<ElementRef>;
  @ViewChildren(NodeComponent) nodeComponents: QueryList<NodeComponent>;
  @ViewChildren(NodeRectComponent) rectNodeComponents: QueryList<NodeRectComponent>;
  @ViewChild(SvgContainerComponent) svgContainer: SvgContainerComponent;
  @ViewChild('graphElement') graphElement: ElementRef;
  @ViewChild('overlayElement') overlayElement: ElementRef;

  constructor(protected cd: ChangeDetectorRef, protected zone: NgZone, private elementRef: ElementRef) {
    super();
  }

  updateNode(id: string, appendNode: NodesItemCorrectInterface) {
    const nodes = this.nodeShape === NodeShape.Rect ? this.rectNodeComponents : this.nodeComponents;
    const node = nodes.find(n => n.id === id);
    if (node) {
      if (this.nodeShape === NodeShape.Rect) {
        const cacheNode = this.cacheNodes.find(n => n.id === id);
        node.update({ ...appendNode, width: cacheNode.width, height: cacheNode.height });
      } else {
        node.update(appendNode);
      }
    }
  }

  updateZoom(value) {
    this.svgContainer.zoomTo(value);
  }

  moveToCenter() {
    if (this.graph && this.graph.graph()) {
      this.visibility = Visibility.Visible;

      const hostDims = this.elementRef.nativeElement.getBoundingClientRect();
      this.cacheTransform.k = Math.min(
        hostDims.height / (this.graph.graph().height + 200),
        hostDims.width / (this.graph.graph().width + 120),
        1
      );
      const width = this.graph.graph().width * this.cacheTransform.k;
      const height = this.graph.graph().height * this.cacheTransform.k;
      let translateX = (hostDims.width - width) / 2;
      let translateY = (hostDims.height - height) / 2;
      if (width < 0 || height < 0) {
        translateX = 0;
        translateY = 0;
      }
      this.zoom = this.cacheTransform.k;
      this.cacheTransform.x = translateX;
      this.cacheTransform.y = translateY;
      const t = zoomIdentity.translate(translateX, translateY).scale(this.zoom);
      this.svgContainer.setPositionByTransform(t);
    }

  }

  flush(nodes, links, isResizeNode = false) {
    this.cacheNodes = [ ...nodes ];
    this.cacheLinks = [ ...links ];
    this.zone.run(() => {
      this.panEnable = true;
      this.selectedNodeId = null;
      this.createGraph({ compound: true });
      this.setNodes(nodes);
      this.setEdge(links);
      requestAnimationFrame(() => {
        this.initLayout(this.nodeShape).then(() => {
          requestAnimationFrame(() => {
            this.redrawLines();
            this.redrawNodes();
            this.moveToCenter();
            if (isResizeNode) {
              this.visibility = Visibility.Hidden;
              setTimeout(() => {
                this.resetNodeSize();
              }, 100);
            }
          });
          this.cd.markForCheck();
        });
      });
    });
  }

  resetNodeSize() {
    if (this.nodeShape === NodeShape.Circle) {
      this.cacheNodes.forEach(n => {
        n.width = 48;
        n.height = 48;
      });
    } else {
      this.graphElement.nativeElement.querySelectorAll(`.node-group`)
      .forEach((nodeEle: HTMLElement) => {
        const contentEle = nodeEle.querySelector('.content-wrap');
        if (contentEle) {
          const height = contentEle.getBoundingClientRect().height;
          const width = contentEle.getBoundingClientRect().width;
          const node = this.cacheNodes.find(n => n.id === nodeEle.id);
          if (node) {
            node.height = height / this.zoom + 30 + 15;
            node.width = width / this.zoom + 30;
          }
        }
      });
    }
    this.flush(this.cacheNodes, this.cacheLinks, false);
  }

  redrawLines(_animate = true): void {
    this.linkElements.map(linkEl => {
      const l = this.layoutLinks.find(lin => lin.id === linkEl.nativeElement.id);
      if (l) {
        const linkSelection = select(linkEl.nativeElement).select('.edge');
        linkSelection
        .attr('d', l.options.oldLine)
        .attr('stroke-width', l.options.oldWidth)
        .transition()
        .duration(_animate ? 500 : 0)
        .attr('d', l.options.line)
        .attr('stroke-width', l.options.width);
      }
    });
  }

  redrawNodes(_animate = true) {
    this.nodeElements.map(nodeEl => {
      const node = this.layoutNodes.find(n => n.id === nodeEl.nativeElement.id);
      if (node) {
        const nodeGroupSelection = select(nodeEl.nativeElement);
        nodeGroupSelection.attr('transform', `${node.options.oldTransform},scale(${node.options.oldScale}, ${node.options.oldScale})`)
        .transition()
        .duration(_animate ? 500 : 0)
        .attr('transform', `${node.options.transform},scale(${node.options.scale}, ${node.options.scale})`);
      }
    });
  }

  focusNode(node: NodesItemCorrectInterface, force = false) {
    const layoutNode = this.layoutNodes.find(n => n.id === node.id);
    this.clickNode(layoutNode, null, force);
  }

  clickNode(e: LayoutNode, $event?: MouseEvent, force = false) {
    if ($event) {
      $event.stopPropagation();
    }
    this.nodeClick.emit(e);
    this.panEnable = false;
    if (!this.selectedNodeId) {
      this.oldTransform = { ...this.svgContainer.containerTransform };
    }
    if (this.selectedNodeId === e.id && !force) {
      return;
    }
    this.selectedNodeId = e.id;
    const hostDims = this.elementRef.nativeElement.getBoundingClientRect();
    const x: number = (hostDims.width / this.xCenter - this.svgContainer.containerTransform.x);
    const y: number = (hostDims.height / this.yCenter - this.svgContainer.containerTransform.y);
    this.zone.run(() => {
      this.zoomFocusLayout({
        x,
        y,
        transform: this.svgContainer.containerTransform,
        nodeId   : e.id,
        zoom     : this.svgContainer ? this.svgContainer.zoom : 1
      }).then(({ transform, focusedLinkIds, circularNodeIds }) => {
        this.focusedLinkIds = [ ...focusedLinkIds ];
        this.circleNodeIds = [ ...circularNodeIds ];
        requestAnimationFrame(() => {
          const t = zoomIdentity.translate(transform.x, transform.y).scale(transform.k);
          this.svgContainer.setPositionByTransform(t, true);
          this.redrawNodes();
        });
        this.graphElement.nativeElement.appendChild(this.overlayElement.nativeElement);
        this.graphElement.nativeElement.querySelectorAll(`.link-group`)
        .forEach(LinkEle => {
          if (this.focusedLinkIds.indexOf(LinkEle.id) !== -1) {
            this.graphElement.nativeElement.appendChild(LinkEle);
          }
        });
        this.graphElement.nativeElement.querySelectorAll(`.node-group`)
        .forEach(nodeEle => {
          if ([ this.selectedNodeId, ...this.circleNodeIds ].indexOf(nodeEle.id) !== -1) {
            this.graphElement.nativeElement.appendChild(nodeEle);
          }
        });
        this.cd.markForCheck();
      });
    });
  }

  clickBg() {
    if (!this.selectedNodeId) {
      return;
    }
    this.selectedNodeId = null;
    this.nodeClick.emit(null);
    this.circleNodeIds = [];
    this.focusedLinkIds = [];
    this.recoveryLayout().then(() => {
      requestAnimationFrame(() => {
        const t = zoomIdentity.translate(this.oldTransform.x, this.oldTransform.y).scale(this.oldTransform.k);
        this.svgContainer.setPositionByTransform(t, true);
        this.redrawLines();
        this.redrawNodes();
      });
      this.panEnable = true;
      this.cd.markForCheck();
    });
  }


  onTransform($event) {
    this.cd.detectChanges();
    this.cacheTransform.x = $event.x;
    this.cacheTransform.y = $event.y;
    this.cacheTransform.k = $event.k;
    this.zoom = $event.k;
  }

  onNodeMouseEnter($event: MouseEvent) {
    this.graphElement.nativeElement.appendChild($event.target);
    this.layoutLinks.forEach(l => {
      if (l.id.split('-').indexOf(($event.target as HTMLElement).id) !== -1) {
        l.options.focused = true;
      }
    });
    this.cd.detectChanges();
  }

  onNodeMouseLeave() {

    this.layoutLinks.forEach(l => {
      l.options.focused = this.focusedLinkIds.indexOf(l.id) !== -1;
    });

    this.graphElement.nativeElement.appendChild(this.overlayElement.nativeElement);

    this.graphElement.nativeElement.querySelectorAll(`.link-group`)
    .forEach(e => {
      if (this.focusedLinkIds.indexOf(e.id) !== -1) {
        this.graphElement.nativeElement.appendChild(e);
      }
    });

    this.graphElement.nativeElement.querySelectorAll(`.node-group`)
    .forEach(e => {
      if ([ this.selectedNodeId, ...this.circleNodeIds ].indexOf(e.id) !== -1) {
        this.graphElement.nativeElement.appendChild(e);
      }
    });

    this.cd.detectChanges();

  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes[ 'nodeShape' ]) {
      this.visibility = Visibility.Hidden;
      setTimeout(() => {
        this.resetNodeSize();
      }, 100);
    }
  }

  ngAfterContentInit(): void {
  }

}
