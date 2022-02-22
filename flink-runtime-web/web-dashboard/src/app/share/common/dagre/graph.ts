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

import { NodesItemCorrect, NodesItemLink } from '@flink-runtime-web/interfaces';
import { curveLinear, line } from 'd3';
import * as dagre from 'dagre';
import { GraphEdge, graphlib } from 'dagre';

import Graph = graphlib.Graph;

export interface LayoutNode extends NodesItemCorrect {
  x: number;
  y: number;
  width: number;
  height: number;
  options: LayoutNodeOptions;
}

export interface LayoutNodeOptions {
  transform: string;
  oldTransform: string;
  scale: number;
  oldScale: number;
  focused: boolean;
}

export interface LayoutLink extends NodesItemLink {
  [key: string]: unknown;

  detail: Record<string, unknown>;
  options: LayoutLinkOptions;
  points: Array<{ x: number; y: number }>;
}

export interface LayoutLinkOptions {
  line: string;
  oldLine: string;
  width: number;
  oldWidth: number;
  focused: boolean;
  dominantBaseline: string;
}

export interface CreateGraphOpt {
  directed?: boolean;
  multigraph?: boolean;
  compound?: boolean;
}

export interface ZoomFocusLayoutOpt {
  nodeId: string;
  zoom: number;
  x: number;
  y: number;
  transform: { x: number; y: number; k: number };
}

export class NzGraph {
  graph: Graph;
  config = {
    ranker: 'network-simplex',
    align: 'DL',
    marginx: 20,
    marginy: 20,
    edgesep: 150,
    ranksep: 150
  };
  copyLayoutNodes: LayoutNode[];
  copyLayoutLinks: LayoutLink[];
  layoutNodes: LayoutNode[] = [];
  layoutLinks: LayoutLink[] = [];

  /**
   * Create graph
   *
   * @param opt
   */
  createGraph(opt: CreateGraphOpt = {}): void {
    this.graph = new dagre.graphlib.Graph(opt);
    this.graph.setGraph({
      rankdir: 'LR',
      ...this.config
    });
    this.graph.setDefaultEdgeLabel(() => ({}));
  }

  /**
   * Set nodes for graph
   *
   * @param nodes
   */
  setNodes(nodes: NodesItemCorrect[]): void {
    nodes.forEach(n => {
      n.width = n.width || 48;
      n.height = n.height || 48;
      this.graph.setNode(n.id, n);
    });
  }

  /**
   * Set links for graph
   *
   * @param links
   */
  setEdge(links: NodesItemLink[]): void {
    links.forEach(l => {
      let length = 0;
      if (l.local_strategy) {
        length += l.local_strategy.length;
      }
      if (l.ship_strategy) {
        length += l.ship_strategy.length;
      }
      l.width = (length || 1) * 3;
      this.graph.setEdge(l.source, l.target, l);
    });
  }

  /**
   * Init graph layout
   */
  initLayout(): Promise<void> {
    if (!this.graph) {
      return Promise.reject();
    }

    this.layoutNodes = [];
    this.copyLayoutNodes = [];
    this.layoutLinks = [];
    this.copyLayoutLinks = [];

    dagre.layout(this.graph);
    const generatedGraph = this.graph.graph();
    if (generatedGraph.width! < generatedGraph.height!) {
      this.graph.setGraph({
        rankdir: 'TB',
        ...this.config
      });
      this.graph.edges().forEach(e => {
        const edge = this.graph.edge(e);
        edge.height = edge.width;
        edge.width = null;
      });
      dagre.layout(this.graph);
    }

    this.graph.nodes().forEach(id => {
      const node = this.graph.node(id) as LayoutNode;
      const transform = `translate(${node.x - node.width / 2 || 0}, ${node.y - 1 / 2 || 0})`;
      node.options = {
        transform,
        oldTransform: transform,
        scale: 1,
        oldScale: 1,
        focused: false
      };

      this.layoutNodes.push({ ...node, options: { ...node.options } });
      this.copyLayoutNodes.push({ ...node, options: { ...node.options } });
    });

    this.graph.edges().forEach(e => {
      const edge = this.graph.edge(e) as LayoutLink & GraphEdge;
      const initLine = this.generateLine(edge.points) as string;
      const link: LayoutLink = {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        points: [...edge.points] as Array<{ x: number; y: number }>,
        options: {
          line: initLine,
          oldLine: initLine,
          width: 1,
          oldWidth: 1,
          focused: false,
          dominantBaseline: this.getDominantBaseline(edge)
        },
        detail: { ...edge }
      };
      this.layoutLinks.push({ ...link, options: { ...link.options } });
      this.copyLayoutLinks.push({ ...link, options: { ...link.options } });
    });

    return Promise.resolve();
  }

  /**
   * Calculate text base line
   *
   * @param edge
   */
  getDominantBaseline(edge: GraphEdge): string {
    const firstPoint = edge.points[0];
    const lastPoint = edge.points[edge.points.length - 1];
    return lastPoint.x < firstPoint.x ? 'rtl' : 'ltr';
  }

  /**
   * Zoom when focus on some node
   *
   * @param opt
   */
  zoomFocusLayout(
    opt: ZoomFocusLayoutOpt
  ): Promise<{ transform: { x: number; y: number; k: number }; focusedLinkIds: string[]; circularNodeIds: string[] }> {
    if (!this.graph.hasNode(opt.nodeId)) {
      console.warn(`node ${opt.nodeId} not exist`);
      return Promise.reject();
    }

    this.layoutNodes.forEach(node => {
      const oNode = this.copyLayoutNodes.find(n => n.id === node.id);
      if (oNode) {
        node.options.oldScale = node.options.scale;
        node.options.scale = oNode.options.scale;
        node.options.focused = false;
      }
    });
    const focusNode = this.layoutNodes.find(n => n.id === opt.nodeId);
    if (focusNode) {
      const circularNodes = this.circleNodes(focusNode);
      focusNode.options.oldScale = focusNode.options.scale;
      focusNode.options.scale = focusNode.options.oldScale * 1.2;
      focusNode.options.focused = true;
      const x = focusNode.x + 45;
      const y = focusNode.y;

      const focusedLinkIds: string[] = [];
      this.layoutLinks.forEach(link => {
        link.options.focused = link.source === opt.nodeId || link.target === opt.nodeId;
        if (link.options.focused) {
          focusedLinkIds.push(link.id);
        }
      });

      return Promise.resolve({
        focusedLinkIds,
        circularNodeIds: circularNodes.map(n => n!.id),
        transform: {
          x: opt.transform.x + opt.x - x,
          y: opt.transform.y + opt.y - y,
          k: 1
        }
      });
    } else {
      return Promise.reject();
    }
  }

  /**
   * Recover layout position
   */
  recoveryLayout(): Promise<void> {
    this.layoutNodes.forEach(node => {
      const oNode = this.copyLayoutNodes.find(n => n.id === node.id);
      if (oNode) {
        node.options.oldTransform = node.options.transform;
        node.options.transform = oNode.options.transform;
        node.options.oldScale = node.options.scale;
        node.options.scale = oNode.options.scale;
        node.x = oNode.x;
        node.y = oNode.y;
        node.options.focused = false;
      }
    });
    this.layoutLinks.forEach(link => {
      link.options.focused = false;
      const oldLink = this.copyLayoutLinks.find(ol => ol.id === link.id);
      if (oldLink) {
        link.points = [...oldLink.points];
        link.options.oldLine = link.options.line;
        link.options.line = oldLink.options.line;
        link.options.oldWidth = link.options.width;
        link.options.width = 1;
      }
    });
    return Promise.resolve();
  }

  /**
   * Get circle node from selected node
   *
   * @param selectedNode
   */
  circleNodes(selectedNode: LayoutNode): LayoutNode[] {
    const nodes = [];
    for (const link of this.layoutLinks) {
      if (link.target === selectedNode.id) {
        const node = this.layoutNodes.find(n => n.id === link.source);
        if (node) {
          nodes.push(node);
        }
      }

      if (link.source === selectedNode.id) {
        const node = this.layoutNodes.find(n => n.id === link.target);
        if (node) {
          nodes.push(node);
        }
      }
    }
    return nodes;
  }

  /**
   * Generate Line from points
   *
   * @param points
   */
  generateLine(points: Array<{ x: number; y: number }>): string | null {
    const transformPoints = points;
    const lineFunction = line()
      .x(d => (d as unknown as { x: number }).x)
      .y(d => (d as unknown as { y: number }).y)
      .curve(curveLinear);
    return lineFunction(transformPoints as unknown as Array<[number, number]>);
  }
}
