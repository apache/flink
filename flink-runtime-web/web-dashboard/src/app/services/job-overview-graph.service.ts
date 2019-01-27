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

import { Injectable } from '@angular/core';
import { scaleLinear } from 'd3';
import {
  GraphDef,
  NzGraphComponent,
  Metaedge,
  RenderGraphInfo,
  RenderGroupNodeInfo,
  RenderNodeInfo,
  RenderMetaedgeInfo, MetanodeImpl
} from '@ng-zorro/ng-plus/graph';
import { JobDetailCorrectInterface, NodesItemCorrectInterface, OperatorsItem, SubtaskMetricsItem, VerticesMetrics } from 'flink-interfaces';

import * as d3 from 'd3';

export interface ViewVerticesDetail {
  parallelism: number;
  inQueue: number;
  outQueue: number;
  displayName: string;
  name: string;
}

export interface ViewOperatorsDetail {
  numRecordsIn: string;
  numRecordsOut: string;
  displayName: string;
  name: string;
  abnormal: boolean;
}

export enum MetricsGetStrategy {
  MAX,
  MIN,
  SUM,
  FIRST
}

const opNameMaxLength = 512;

const getLabelForEdge = (metaedge: Metaedge,
                         renderInfo: RenderGraphInfo): string => {
  if (Object.keys(renderInfo.getSubhierarchy()).length === 1) {
    return metaedge.baseEdgeList[ 0 ][ 'partitioner' ] || null;
  }
  return null;
};

const edgesLayoutFunction = (graph: graphlib.Graph<RenderNodeInfo, RenderMetaedgeInfo>,
                             params): void => {
  graph.edges().forEach(e => {
    const edge = graph.edge(e) as any;
    if (!edge.structural) {
      const maxLabelLength = Math.max(edge.metaedge.baseEdgeList.map(_e => (_e.partitioner || '').length));
      const rankdir = graph.graph().rankdir;
      const rankSep = edge.metaedge.inbound ? graph.graph().ranksep : Math.max(params.rankSep, maxLabelLength * 5);
      if ([ 'RL', 'LR' ].indexOf(rankdir) !== -1) {
        edge.width = rankSep;
      } else {
        edge.height = rankSep;
      }
    }
  });
};

const opNodeHeightFunction = (renderNodeInfo: RenderNodeInfo): number => {
  const heightRange = scaleLinear().domain([ 1, 2, 3 ]).range([ 85, 100, 115 ] as ReadonlyArray<number>);
  const nameLength = Math.min(opNameMaxLength, renderNodeInfo.node.attr[ 'name' ].length);
  return heightRange(Math.ceil((nameLength + 3) / 28));
};

const canToggleExpand = (renderNodeInfo: RenderGroupNodeInfo): boolean => {
  const children = (renderNodeInfo.node as MetanodeImpl).getChildren();
  return !(children.length === 1 && children[ 0 ].attr[ 'virtual' ]);
};

export const graphTimeoutRange = scaleLinear().domain([ 50, 100, 300, 500 ])
.range([ 250, 500, 800, 1000 ] as ReadonlyArray<number>).clamp(true);


@Injectable({
  providedIn: 'root'
})
export class JobOverviewGraphService {

  sourceData: JobDetailCorrectInterface;
  graphComponent: NzGraphComponent;
  graphDef: GraphDef;
  verticesDetailsCache = new Map<RenderGroupNodeInfo, ViewVerticesDetail>();
  operatorsDetailsCache = new Map<RenderNodeInfo, ViewOperatorsDetail>();
  transformCache: { x: number, y: number, k: number };

  getLabelForEdge = getLabelForEdge;
  edgesLayoutFunction = edgesLayoutFunction;
  opNodeHeightFunction = opNodeHeightFunction;
  canToggleExpand = canToggleExpand;
  groupNodeHeightFunction = () => 165;

  constructor() {
  }

  cleanDetailCache() {
    this.verticesDetailsCache.clear();
    this.operatorsDetailsCache.clear();
  }

  setTransformCache() {
    if (this.transformCache || !this.graphComponent.zoom.zoomTransform) {
      return;
    }
    const { x, y, k } = this.graphComponent.zoom.zoomTransform;
    this.transformCache = {
      x,
      y,
      k
    };
  }

  resetTransform(graphComponent: NzGraphComponent) {
    if (!this.transformCache) {
      if (graphComponent && graphComponent.fit) {
        graphComponent.fit();
      }
      return;
    }
    const transform = d3.zoomIdentity
    .scale(this.transformCache.k)
    .translate(this.transformCache.x / this.transformCache.k, this.transformCache.y / this.transformCache.k);

    d3.select(this.graphComponent.zoom.containerEle)
    .transition().duration(500)
    .call(this.graphComponent.zoom.zoom.transform, transform);
    this.transformCache = null;
  }

  initGraph(graphComponent: NzGraphComponent, data: JobDetailCorrectInterface) {
    const graphDef = this.parseGraphData(data);
    this.cleanDetailCache();
    graphComponent.buildGraph(graphDef)
    .then(graph => graphComponent.buildRenderGraphInfo(graph))
    .then(() => {
      if (this.graphComponent) {
        this.graphComponent.clean();
      }
      graphComponent.build();
      this.graphComponent = graphComponent;
      setTimeout(() => {
        graphComponent.fit(0, .8);
      }, (data.plan && data.plan.nodes) ? graphTimeoutRange(data.plan.nodes.length) : 200);
    });
  }

  updateData(data: JobDetailCorrectInterface) {
    this.sourceData = data;
    this.operatorsDetailsCache.forEach((v, k) => {
      Object.assign(v, this.getOperatorsDetail(k, true));
      this.graphComponent.emitChangeByNodeInfo(k);
    });
    this.verticesDetailsCache.forEach((v, k) => {
      Object.assign(v, this.getVerticesDetail(k, true));
      this.graphComponent.emitChangeByNodeInfo(k);
    });
  }

  parseGraphData(data: JobDetailCorrectInterface): GraphDef {
    this.sourceData = data;
    const nodes = [];
    const getNamespaces = operatorId => {
      const op = data.verticesDetail.operators.find(e => e.operator_id === operatorId);
      return op.vertex_id ? `${op.vertex_id}/${op.operator_id}` : op.operator_id;
    };

    data.verticesDetail.operators.forEach(op => {
      nodes.push({
        name  : getNamespaces(op.operator_id),
        inputs: op.inputs.map(e => {
          return {
            name: getNamespaces(e.operator_id),
            attr: { ...e }
          };
        }),
        attr  : { ...op }
      });
    });

    this.graphDef = {
      nodes
    };
    return this.graphDef;
  }

  getVerticesDetail(nodeRenderInfo: RenderGroupNodeInfo, force = false): ViewVerticesDetail {
    if (this.verticesDetailsCache.has(nodeRenderInfo) && !force) {
      return this.verticesDetailsCache.get(nodeRenderInfo);
    }
    const vertices = this.sourceData.verticesDetail.vertices.find(v => v.id === nodeRenderInfo.node.name);
    if (!vertices) {
      return null;
    }

    let displayName = '';
    let inQueue = null;
    let outQueue = null;
    if (vertices.name) {
      displayName = vertices.name.length > 125 ? `${vertices.name.substring(0, 125)}...` : vertices.name;
    } else {
      displayName = vertices.name;
    }

    if (vertices.metrics && Number.isFinite(vertices.metrics[ 'buffers-in-pool-usage-max' ])) {
      inQueue = vertices.metrics[ 'buffers-in-pool-usage-max' ] === -1
        ? null
        : vertices.metrics[ 'buffers-in-pool-usage-max' ];
    } else {
      inQueue = Math.max(
        ...vertices.subtask_metrics
        .map(m => this.parseFloat(m[ 'buffers.inPoolUsage' ]))
      );
    }

    if (vertices.metrics && Number.isFinite(vertices.metrics[ 'buffers-out-pool-usage-max' ])) {
      outQueue = vertices.metrics[ 'buffers-out-pool-usage-max' as keyof VerticesMetrics ] === -1
        ? null
        : vertices.metrics[ 'buffers-out-pool-usage-max' ];
    } else {
      outQueue = Math.max(
        ...vertices.subtask_metrics
        .map(m => this.parseFloat(m[ 'buffers.outPoolUsage' ]))
      );
    }

    this.verticesDetailsCache.set(nodeRenderInfo, {
      displayName,
      name       : vertices.name,
      inQueue    : Number.isFinite(inQueue) ? inQueue : null,
      outQueue   : Number.isFinite(outQueue) ? outQueue : null,
      parallelism: this.parseFloat(vertices.parallelism) || vertices.subtask_metrics.length
    });

    return this.verticesDetailsCache.get(nodeRenderInfo);
  }

  getOperatorsDetail(nodeRenderInfo: RenderNodeInfo, force = false): ViewOperatorsDetail {
    if (this.operatorsDetailsCache.has(nodeRenderInfo) && !force) {
      return this.operatorsDetailsCache.get(nodeRenderInfo);
    }
    const operator = this.sourceData.verticesDetail.operators
    .find(o => o.operator_id === nodeRenderInfo.node.attr[ 'operator_id' ]);

    if (!operator) {
      return null;
    }

    let displayName = '';
    if (operator.name.length > opNameMaxLength) {
      displayName = `${operator.name.substring(0, opNameMaxLength)}...`;
    } else {
      displayName = operator.name;
    }

    const vertices = this.sourceData.verticesDetail.vertices.find(v => v.id === operator.vertex_id);

    const numRecordsIn = this.getMetric(vertices.subtask_metrics, operator, 'numRecordsInOperator', MetricsGetStrategy.SUM);
    const numRecordsOut = this.getMetric(vertices.subtask_metrics, operator, 'numRecordsOutOperator', MetricsGetStrategy.SUM);

    const abnormal = !/^Sink:\s.+$/.test(operator.name)
      && Number.isFinite(numRecordsIn)
      && Number.isFinite(numRecordsOut)
      && numRecordsIn > 0
      && numRecordsOut <= 0;

    this.operatorsDetailsCache.set(nodeRenderInfo, {
        abnormal,
        displayName,
        name         : operator.name,
        numRecordsIn : Number.isFinite(numRecordsIn) ? `${numRecordsIn}` : ' - ',
        numRecordsOut: Number.isFinite(numRecordsOut) ? `${numRecordsOut}` : ' - '
      }
    );

    return this.operatorsDetailsCache.get(nodeRenderInfo);
  }

  parseFloat(value: number | string): number {
    if (typeof value === 'number') {
      return value;
    } else {
      const n = Number.parseFloat(value);
      return Number.isFinite(n) ? n : null;
    }
  }

  getNodesItemCorrect(name: string): NodesItemCorrectInterface {
    return this.sourceData.plan.nodes.find(n => n.id === name);
  }


  getMetric(metrics: SubtaskMetricsItem[], operator: OperatorsItem, metricKey: string, strategy: MetricsGetStrategy) {
    // If can't use operator_id, use metric_name
    const canUseId = metrics.some(m => !!m[ `${operator.operator_id}.${metricKey}` ]);
    const spliceKey = `${canUseId ? operator.operator_id : operator.metric_name}.${metricKey}`;
    switch (strategy) {
      case MetricsGetStrategy.MAX:
        return Math.max(
          ...metrics.map(m => this.parseFloat(m[ spliceKey ]))
        );
      case MetricsGetStrategy.MIN:
        return Math.min(
          ...metrics.map(m => this.parseFloat(m[ spliceKey ]))
        );
      case MetricsGetStrategy.SUM:
        return metrics.map(m => this.parseFloat(m[ spliceKey ])).reduce((a, b) => a + b, 0);
      case MetricsGetStrategy.FIRST:
        return this.parseFloat(metrics[ 0 ][ spliceKey ]);
      default:
        return null;
    }
  }
}
