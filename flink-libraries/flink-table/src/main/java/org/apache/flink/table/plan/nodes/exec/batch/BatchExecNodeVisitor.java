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

package org.apache.flink.table.plan.nodes.exec.batch;

import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
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
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSink;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortMergeJoinBase;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTableSourceScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecTemporalTableJoin;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecValues;

/**
 * Visitor pattern for traversing a dag of {@link BatchExecNode} objects. The visit can see the
 * detail type of every node.
 */
public abstract class BatchExecNodeVisitor {

	//~ visit Methods ----------------------------------------------------------------

	public void visit(BatchExecBoundedStreamScan boundedStreamScan) {
		visitInputs(boundedStreamScan);
	}

	public void visit(BatchExecTableSourceScan scanTableSource) {
		visitInputs(scanTableSource);
	}

	public void visit(BatchExecValues values) {
		visitInputs(values);
	}

	public void visit(BatchExecCalc calc) {
		visitInputs(calc);
	}

	public void visit(BatchExecCorrelate correlate) {
		visitInputs(correlate);
	}

	public void visit(BatchExecExchange exchange) {
		visitInputs(exchange);
	}

	public void visit(BatchExecExpand expand) {
		visitInputs(expand);
	}

	public void visit(BatchExecHashAggregate hashAggregate) {
		visitInputs(hashAggregate);
	}

	public void visit(BatchExecHashWindowAggregate hashAggregate) {
		visitInputs(hashAggregate);
	}

	public void visit(BatchExecHashJoinBase hashJoin) {
		visitInputs(hashJoin);
	}

	public void visit(BatchExecSortMergeJoinBase sortMergeJoin) {
		visitInputs(sortMergeJoin);
	}

	public void visit(BatchExecNestedLoopJoinBase nestedLoopJoin) {
		visitInputs(nestedLoopJoin);
	}

	public void visit(BatchExecLocalHashAggregate localHashAggregate) {
		visitInputs(localHashAggregate);
	}

	public void visit(BatchExecSortAggregate sortAggregate) {
		visitInputs(sortAggregate);
	}

	public void visit(BatchExecLocalHashWindowAggregate localHashAggregate) {
		visitInputs(localHashAggregate);
	}

	public void visit(BatchExecLocalSortAggregate localSortAggregate) {
		visitInputs(localSortAggregate);
	}

	public void visit(BatchExecLocalSortWindowAggregate localSortAggregate) {
		visitInputs(localSortAggregate);
	}

	public void visit(BatchExecSortWindowAggregate sortAggregate) {
		visitInputs(sortAggregate);
	}

	public void visit(BatchExecOverAggregate overWindowAgg) {
		visitInputs(overWindowAgg);
	}

	public void visit(BatchExecLimit limit) {
		visitInputs(limit);
	}

	public void visit(BatchExecSort sort) {
		visitInputs(sort);
	}

	public void visit(BatchExecSortLimit sortLimit) {
		visitInputs(sortLimit);
	}

	public void visit(BatchExecRank rank) {
		visitInputs(rank);
	}

	public void visit(BatchExecUnion union) {
		visitInputs(union);
	}

	public void visit(BatchExecTemporalTableJoin joinTable) {
		visitInputs(joinTable);
	}

	public void visit(BatchExecSink<?> sink) {
		visitInputs(sink);
	}

	protected void visitInputs(BatchExecNode<?> node) {
		node.getInputNodes().forEach(n -> ((BatchExecNode<?>) n).accept(this));
	}
}
