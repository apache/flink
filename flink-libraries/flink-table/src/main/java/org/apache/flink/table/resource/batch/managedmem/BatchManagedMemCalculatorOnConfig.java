/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch.managedmem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregateBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregateBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecNestedLoopJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecOverAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRank;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTemporalTableJoin;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;
import org.apache.flink.table.util.NodeResourceUtil;

/**
 * Default managed memory calculator for batch node.
 */
public class BatchManagedMemCalculatorOnConfig extends BatchExecNodeVisitor {

	private final Configuration tableConf;

	public BatchManagedMemCalculatorOnConfig(Configuration tableConf) {
		this.tableConf = tableConf;
	}

	private void calculateNoManagedMem(BatchExecNode<?> batchExecNode) {
		super.visitInputs(batchExecNode);
		batchExecNode.getResource().setManagedMem(0, 0, 0);
	}

	@Override
	public void visit(BatchExecBoundedStreamScan boundedStreamScan) {
		calculateNoManagedMem(boundedStreamScan);
	}

	@Override
	public void visit(BatchExecTableSourceScan scanTableSource) {
		calculateNoManagedMem(scanTableSource);
	}

	@Override
	public void visit(BatchExecValues values) {
		calculateNoManagedMem(values);
	}

	@Override
	public void visit(BatchExecCalc calc) {
		calculateNoManagedMem(calc);
	}

	@Override
	public void visit(BatchExecCorrelate correlate) {
		calculateNoManagedMem(correlate);
	}

	@Override
	public void visit(BatchExecExchange exchange) {
		super.visitInputs(exchange);
	}

	@Override
	public void visit(BatchExecExpand expand) {
		calculateNoManagedMem(expand);
	}

	private void calculateHashAgg(BatchExecHashAggregateBase hashAgg) {
		if (hashAgg.getGrouping().length == 0) {
			calculateNoManagedMem(hashAgg);
			return;
		}
		super.visitInputs(hashAgg);
		int	reservedMem = NodeResourceUtil.getHashAggManagedMemory(tableConf);
		int	preferMem = NodeResourceUtil.getHashAggManagedPreferredMemory(tableConf);
		int	maxMem = NodeResourceUtil.getHashAggManagedMaxMemory(tableConf);
		hashAgg.getResource().setManagedMem(reservedMem, preferMem, maxMem);
	}

	private void calculateHashWindowAgg(BatchExecHashWindowAggregateBase hashWindowAgg) {
		super.visitInputs(hashWindowAgg);
		int reservedMem = NodeResourceUtil.getHashAggManagedMemory(tableConf);
		int preferMem = NodeResourceUtil.getHashAggManagedPreferredMemory(tableConf);
		int	maxMem = NodeResourceUtil.getHashAggManagedMaxMemory(tableConf);
		hashWindowAgg.getResource().setManagedMem(reservedMem, preferMem, maxMem);
	}

	@Override
	public void visit(BatchExecHashAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateHashWindowAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashJoinBase hashJoin) {
		super.visitInputs(hashJoin);
		int reservedMem = NodeResourceUtil.getHashJoinTableManagedMemory(tableConf);
		int preferMem = NodeResourceUtil.getHashJoinTableManagedPreferredMemory(tableConf);
		int maxMem = NodeResourceUtil.getHashJoinTableManagedMaxMemory(tableConf);
		hashJoin.getResource().setManagedMem(reservedMem, preferMem, maxMem);
	}

	@Override
	public void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		super.visitInputs(sortMergeJoin);
		int externalBufferMemoryMb = NodeResourceUtil.getExternalBufferManagedMemory(
				tableConf) * sortMergeJoin.getExternalBufferNum();
		int sortMemory = NodeResourceUtil.getSortBufferManagedMemory(tableConf);
		int reservedMemory = sortMemory * 2 + externalBufferMemoryMb;
		int preferSortMemory = NodeResourceUtil.getSortBufferManagedPreferredMemory(
				tableConf);
		int preferMemory = preferSortMemory * 2 + externalBufferMemoryMb;
		int maxSortMemory = NodeResourceUtil.getSortBufferManagedMaxMemory(tableConf);
		int maxMemory = maxSortMemory * 2 + externalBufferMemoryMb;
		sortMergeJoin.getResource().setManagedMem(reservedMemory, preferMemory, maxMemory);
	}

	@Override
	public void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			super.visitInputs(nestedLoopJoin);
			int externalBufferMemoryMb = NodeResourceUtil.getExternalBufferManagedMemory(tableConf);
			nestedLoopJoin.getResource().setManagedMem(externalBufferMemoryMb, externalBufferMemoryMb, externalBufferMemoryMb);
		}
	}

	@Override
	public void visit(BatchExecLocalHashAggregate localHashAggregate) {
		calculateHashAgg(localHashAggregate);
	}

	@Override
	public void visit(BatchExecSortAggregate sortAggregate) {
		calculateNoManagedMem(sortAggregate);
	}

	@Override
	public void visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		calculateHashWindowAgg(localHashAggregate);
	}

	@Override
	public void visit(BatchExecLocalSortAggregate localSortAggregate) {
		calculateNoManagedMem(localSortAggregate);
	}

	@Override
	public void visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		calculateNoManagedMem(localSortAggregate);
	}

	@Override
	public void visit(BatchExecSortWindowAggregate sortAggregate) {
		calculateNoManagedMem(sortAggregate);
	}

	@Override
	public void visit(BatchExecOverAggregate overWindowAgg) {
		boolean[] needBufferList = overWindowAgg.needBufferDataToNeedResetAcc()._1;
		boolean needBuffer = false;
		for (boolean b : needBufferList) {
			if (b) {
				needBuffer = true;
				break;
			}
		}
		if (!needBuffer) {
			calculateNoManagedMem(overWindowAgg);
		} else {
			super.visitInputs(overWindowAgg);
			int externalBufferMemory = NodeResourceUtil.getExternalBufferManagedMemory(tableConf);
			overWindowAgg.getResource().setManagedMem(externalBufferMemory, externalBufferMemory, externalBufferMemory);
		}
	}

	@Override
	public void visit(BatchExecLimit limit) {
		calculateNoManagedMem(limit);
	}

	@Override
	public void visit(BatchExecSort sort) {
		super.visitInputs(sort);
		int reservedMemory = NodeResourceUtil.getSortBufferManagedMemory(tableConf);
		int preferMemory = NodeResourceUtil.getSortBufferManagedPreferredMemory(tableConf);
		int maxMemory = NodeResourceUtil.getSortBufferManagedMaxMemory(tableConf);
		sort.getResource().setManagedMem(reservedMemory, preferMemory, maxMemory);
	}

	@Override
	public void visit(BatchExecSortLimit sortLimit) {
		calculateNoManagedMem(sortLimit);
	}

	@Override
	public void visit(BatchExecRank rank) {
		calculateNoManagedMem(rank);
	}

	@Override
	public void visit(BatchExecUnion union) {
		super.visitInputs(union);
	}

	@Override
	public void visit(BatchExecTemporalTableJoin joinTable) {
		calculateNoManagedMem(joinTable);
	}
}
