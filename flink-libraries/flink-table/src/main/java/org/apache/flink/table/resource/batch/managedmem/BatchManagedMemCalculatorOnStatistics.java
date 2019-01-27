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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCorrelate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExpand;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecHashWindowAggregate;
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
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.runtime.sort.BinaryExternalSorter.SORTER_MIN_NUM_SORT_MEM;
import static org.apache.flink.table.util.NodeResourceUtil.SQL_RESOURCE_INFER_OPERATOR_MEMORY_MIN;

/**
 * Managed memory calculator on statistics for batch node.
 */
public class BatchManagedMemCalculatorOnStatistics extends BatchExecNodeVisitor {

	private final Configuration tableConf;

	public BatchManagedMemCalculatorOnStatistics(
			Configuration tableConf) {
		this.tableConf = tableConf;
	}

	private int getResultPartitionCount(BatchExecNode<?> batchExecNode) {
		return batchExecNode.getResource().getParallelism();
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

	private void calculateHashAgg(BatchExecNode<?> hashAgg) {
		super.visitInputs(hashAgg);
		double memoryInBytes = hashAgg.getEstimatedTotalMem();
		Tuple3<Integer, Integer, Integer> managedMem = NodeResourceUtil.reviseAndGetInferManagedMem(
				tableConf, (int) (memoryInBytes / NodeResourceUtil.SIZE_IN_MB / getResultPartitionCount(hashAgg)));
		hashAgg.getResource().setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
	}

	@Override
	public void visit(BatchExecHashAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashWindowAggregate hashAggregate) {
		calculateHashAgg(hashAggregate);
	}

	@Override
	public void visit(BatchExecHashJoinBase hashJoin) {
		super.visitInputs(hashJoin);
		int shuffleCount = hashJoin.shuffleBuildCount(hashJoin.getFlinkPhysicalRel().getCluster().getMetadataQuery());
		int memCostInMb = (int) (hashJoin.getEstimatedTotalMem() /
				shuffleCount / NodeResourceUtil.SIZE_IN_MB);
		Tuple3<Integer, Integer, Integer> managedMem = NodeResourceUtil.reviseAndGetInferManagedMem(
				tableConf, memCostInMb / getResultPartitionCount(hashJoin));
		hashJoin.getResource().setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
	}

	@Override
	public void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		super.visitInputs(sortMergeJoin);
		int externalBufferMemoryMb = NodeResourceUtil.getExternalBufferManagedMemory(
				tableConf) * sortMergeJoin.getExternalBufferNum();
		double memoryInBytes = sortMergeJoin.getEstimatedTotalMem();
		Tuple3<Integer, Integer, Integer> managedMem = NodeResourceUtil.reviseAndGetInferManagedMem(
				tableConf,
				(int) (memoryInBytes / NodeResourceUtil.SIZE_IN_MB / getResultPartitionCount(sortMergeJoin)));
		int reservedMemory = managedMem.f0 + externalBufferMemoryMb;
		int preferMemory = managedMem.f1 + externalBufferMemoryMb;
		int configMinMemory = NodeResourceUtil.getOperatorMinManagedMem(tableConf);
		int minSortMemory = (int) (SORTER_MIN_NUM_SORT_MEM * 2 / NodeResourceUtil.SIZE_IN_MB) + 1;
		Preconditions.checkArgument(configMinMemory >= externalBufferMemoryMb + minSortMemory,
				SQL_RESOURCE_INFER_OPERATOR_MEMORY_MIN +
						" should >= externalBufferMemoryMb(" +
						externalBufferMemoryMb +
						"), minSortMemory(" +
						minSortMemory + ").");
		sortMergeJoin.getResource().setManagedMem(reservedMemory, preferMemory, managedMem.f2);
	}

	@Override
	public void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		if (nestedLoopJoin.singleRowJoin()) {
			calculateNoManagedMem(nestedLoopJoin);
		} else {
			super.visitInputs(nestedLoopJoin);
			int shuffleCount = nestedLoopJoin.shuffleBuildCount(nestedLoopJoin.getFlinkPhysicalRel().getCluster().getMetadataQuery());
			double memCostInMb = nestedLoopJoin.getEstimatedTotalMem() /
					shuffleCount / NodeResourceUtil.SIZE_IN_MB;
			Tuple3<Integer, Integer, Integer> managedMem = NodeResourceUtil.reviseAndGetInferManagedMem(
					tableConf, (int) (memCostInMb / getResultPartitionCount(nestedLoopJoin)));
			nestedLoopJoin.getResource().setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
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
		calculateHashAgg(localHashAggregate);
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
		double memoryInBytes = sort.getEstimatedTotalMem();
		Tuple3<Integer, Integer, Integer> managedMem = NodeResourceUtil.reviseAndGetInferManagedMem(
				tableConf, (int) (memoryInBytes / NodeResourceUtil.SIZE_IN_MB / getResultPartitionCount(sort)));
		sort.getResource().setManagedMem(managedMem.f0, managedMem.f1, managedMem.f2);
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
