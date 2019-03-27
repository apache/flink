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
  EventEmitter,
  Input,
  NgZone,
  Output,
  QueryList,
  ViewChild,
  ViewChildren
} from '@angular/core';

import { select } from 'd3-selection';
import { zoomIdentity } from 'd3-zoom';
import { NodesItemCorrectInterface, NodesItemLinkInterface } from 'interfaces';
import { LayoutNode, NzGraph } from './graph';
import { NodeComponent } from './node.component';
import { SvgContainerComponent } from './svg-container.component';

enum Visibility {
  Hidden = 'hidden',
  Visible = 'visible'
}

@Component({
  selector: 'flink-dagre',
  templateUrl: './dagre.component.html',
  styleUrls: ['./dagre.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class DagreComponent extends NzGraph {
  visibility: Visibility | string = Visibility.Hidden;
  circleNodeIds: string[] = [];
  focusedLinkIds: string[] = [];
  selectedNodeId: string | null;
  zoom = 1;
  cacheTransform = { x: 0, y: 0, k: 1 };
  oldTransform = { x: 0, y: 0, k: 1 };
  cacheNodes: NodesItemCorrectInterface[] = [];
  cacheLinks: NodesItemLinkInterface[] = [];
  @ViewChildren('nodeElement') nodeElements: QueryList<ElementRef>;
  @ViewChildren('linkElement') linkElements: QueryList<ElementRef>;
  @ViewChildren(NodeComponent) rectNodeComponents: QueryList<NodeComponent>;
  @ViewChild(SvgContainerComponent) svgContainer: SvgContainerComponent;
  @ViewChild('graphElement') graphElement: ElementRef;
  @ViewChild('overlayElement') overlayElement: ElementRef;
  @Input() xCenter = 2;
  @Input() yCenter = 2;
  @Output() nodeClick = new EventEmitter<LayoutNode | null>();

  /**
   * Update Node detail
   * @param id
   * @param appendNode
   */
  updateNode(id: string, appendNode: NodesItemCorrectInterface) {
    const nodes = this.rectNodeComponents;
    const node = nodes.find(n => n.id === id);
    if (node) {
      const cacheNode = this.cacheNodes.find(n => n.id === id);
      node.update({ ...appendNode, width: cacheNode!.width, height: cacheNode!.height });
    }
  }

  /**
   * Focus on specific node
   * @param node
   * @param force
   */
  focusNode(node: NodesItemCorrectInterface, force = false) {
    const layoutNode = this.layoutNodes.find(n => n.id === node.id);
    this.clickNode(layoutNode, null, force, false);
  }

  /**
   * Zoom to level
   * @param value
   */
  zoomToLevel(value: number) {
    this.svgContainer.zoomTo(value);
  }

  /**
   * Move graph to center
   */
  moveToCenter() {
    if (this.graph && this.graph.graph()) {
      this.visibility = Visibility.Visible;

      const hostDims = this.elementRef.nativeElement.getBoundingClientRect();
      const generatedGraph = this.graph.graph() as any;
      this.cacheTransform.k = Math.min(
        hostDims.height / (generatedGraph.height + 200),
        hostDims.width / (generatedGraph.width + 120),
        1
      );
      const width = generatedGraph.width * this.cacheTransform.k;
      const height = generatedGraph.height * this.cacheTransform.k;
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

  /**
   * Flush graph with nodes and links
   * @param nodes
   * @param links
   * @param isResizeNode
   */
  flush(nodes: NodesItemCorrectInterface[], links: NodesItemLinkInterface[], isResizeNode = false): Promise<void> {
    return new Promise(resolve => {
      this.cacheNodes = [...nodes];
      this.cacheLinks = [...links];
      this.zone.run(() => {
        this.selectedNodeId = null;
        this.createGraph({ compound: true });
        this.setNodes(nodes);
        this.setEdge(links);
        requestAnimationFrame(() => {
          this.initLayout().then(() => {
            requestAnimationFrame(() => {
              this.redrawLines();
              this.redrawNodes();
              this.moveToCenter();
              if (isResizeNode) {
                this.visibility = Visibility.Hidden;
                setTimeout(() => {
                  this.resetNodeSize().then(() => resolve());
                }, 300);
              } else {
                resolve();
              }
            });
            this.cd.markForCheck();
          });
        });
      });
    });
  }

  trackByLink(_: number, link: NodesItemLinkInterface) {
    return link.id;
  }

  trackByNode(_: number, link: NodesItemCorrectInterface) {
    return link.id;
  }

  /**
   * Calculate node size
   */
  resetNodeSize() {
    this.graphElement.nativeElement.querySelectorAll(`.node-group`).forEach((nodeEle: HTMLElement) => {
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
    return this.flush(this.cacheNodes, this.cacheLinks, false);
  }

  /**
   * Redraw all lines
   * @param animate
   */
  redrawLines(animate = true): void {
    this.linkElements.map(linkEl => {
      const l = this.layoutLinks.find(lin => lin.id === linkEl.nativeElement.id);
      if (l) {
        const linkSelection = select(linkEl.nativeElement).select('.edge');
        linkSelection
          .attr('d', l.options.oldLine)
          .attr('stroke-width', l.options.oldWidth)
          .transition()
          .duration(animate ? 500 : 0)
          .attr('d', l.options.line)
          .attr('stroke-width', l.options.width);
      }
    });
  }

  /**
   * Redraw all nodes
   * @param animate
   */
  redrawNodes(animate = true) {
    this.nodeElements.map(nodeEl => {
      const node = this.layoutNodes.find(n => n.id === nodeEl.nativeElement.id);
      if (node) {
        const nodeGroupSelection = select(nodeEl.nativeElement);
        if (animate) {
          nodeGroupSelection
            .attr('transform', `${node.options.oldTransform},scale(${node.options.oldScale}, ${node.options.oldScale})`)
            .transition()
            .duration(500)
            .attr('transform', `${node.options.transform},scale(${node.options.scale}, ${node.options.scale})`);
        } else {
          nodeGroupSelection.attr(
            'transform',
            `${node.options.transform},scale(${node.options.scale}, ${node.options.scale})`
          );
        }
      }
    });
  }

  /**
   * Click specific node
   * @param node
   * @param $event
   * @param force
   */
  clickNode(node: LayoutNode | undefined, $event?: MouseEvent | null, force = false, emit = true) {
    if ($event) {
      $event.stopPropagation();
    }
    if (node) {
      if (emit) {
        this.nodeClick.emit(node);
      }
      if (!this.selectedNodeId) {
        this.oldTransform = { ...this.svgContainer.containerTransform };
      }
      if (this.selectedNodeId === node.id && !force) {
        return;
      }
      this.selectedNodeId = node.id;
      const hostDims = this.elementRef.nativeElement.getBoundingClientRect();
      const x: number = hostDims.width / this.xCenter - this.svgContainer.containerTransform.x;
      const y: number = hostDims.height / this.yCenter - this.svgContainer.containerTransform.y;
      this.zone.run(() => {
        this.zoomFocusLayout({
          x,
          y,
          transform: this.svgContainer.containerTransform,
          nodeId: node.id,
          zoom: this.svgContainer ? this.svgContainer.zoom : 1
        }).then(({ transform, focusedLinkIds, circularNodeIds }) => {
          this.focusedLinkIds = [...focusedLinkIds];
          this.circleNodeIds = [...circularNodeIds];
          requestAnimationFrame(() => {
            const t = zoomIdentity.translate(transform.x, transform.y).scale(transform.k);
            this.svgContainer.setPositionByTransform(t, true);
            this.redrawNodes(!force);
          });
          this.graphElement.nativeElement.appendChild(this.overlayElement.nativeElement);
          this.graphElement.nativeElement.querySelectorAll(`.link-group`).forEach((LinkEle: Element) => {
            if (this.focusedLinkIds.indexOf(LinkEle.id) !== -1) {
              this.graphElement.nativeElement.appendChild(LinkEle);
            }
          });
          this.graphElement.nativeElement.querySelectorAll(`.node-group`).forEach((nodeEle: Element) => {
            if ([this.selectedNodeId, ...this.circleNodeIds].indexOf(nodeEle.id) !== -1) {
              this.graphElement.nativeElement.appendChild(nodeEle);
            }
          });
          this.cd.markForCheck();
        });
      });
    }
  }

  /**
   * Redraw graph
   */
  redrawGraph() {
    this.selectedNodeId = null;
    this.circleNodeIds = [];
    this.focusedLinkIds = [];
    this.recoveryLayout().then(() => {
      requestAnimationFrame(() => {
        if (this.oldTransform.x === 0 && this.oldTransform.y === 0 && this.oldTransform.k === 1) {
          this.moveToCenter();
        } else {
          const t = zoomIdentity.translate(this.oldTransform.x, this.oldTransform.y).scale(this.oldTransform.k);
          this.svgContainer.setPositionByTransform(t, true);
          this.redrawLines();
          this.redrawNodes();
        }
      });
      this.cd.markForCheck();
    });
  }

  /**
   * Handle svg container transform event
   * @param $event
   */
  onTransform($event: { x: number; y: number; k: number }) {
    this.cd.detectChanges();
    this.cacheTransform.x = $event.x;
    this.cacheTransform.y = $event.y;
    this.cacheTransform.k = $event.k;
    this.zoom = $event.k;
  }

  /**
   * Mouse enter node event
   * @param $event
   */
  onNodeMouseEnter($event: MouseEvent) {
    this.graphElement.nativeElement.appendChild($event.target);
    this.layoutLinks.forEach(l => {
      if (l.id.split('-').indexOf(($event.target as HTMLElement).id) !== -1) {
        l.options.focused = true;
      }
    });
    this.cd.detectChanges();
  }

  /**
   * Mouse leave node event
   */
  onNodeMouseLeave() {
    this.layoutLinks.forEach(l => {
      l.options.focused = this.focusedLinkIds.indexOf(l.id) !== -1;
    });

    this.graphElement.nativeElement.appendChild(this.overlayElement.nativeElement);

    this.graphElement.nativeElement.querySelectorAll(`.link-group`).forEach((e: Element) => {
      if (this.focusedLinkIds.indexOf(e.id) !== -1) {
        this.graphElement.nativeElement.appendChild(e);
      }
    });

    this.graphElement.nativeElement.querySelectorAll(`.node-group`).forEach((e: Element) => {
      if ([this.selectedNodeId, ...this.circleNodeIds].indexOf(e.id) !== -1) {
        this.graphElement.nativeElement.appendChild(e);
      }
    });

    this.cd.detectChanges();
  }

  constructor(protected cd: ChangeDetectorRef, protected zone: NgZone, private elementRef: ElementRef) {
    super();
  }
}
