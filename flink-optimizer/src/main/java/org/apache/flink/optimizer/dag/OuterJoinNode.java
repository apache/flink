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

package org.apache.flink.optimizer.dag;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase;
import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.AbstractJoinDescriptor;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.SortMergeFullOuterJoinDescriptor;
import org.apache.flink.optimizer.operators.SortMergeLeftOuterJoinDescriptor;
import org.apache.flink.optimizer.operators.SortMergeRightOuterJoinDescriptor;

import java.util.ArrayList;
import java.util.List;

public class OuterJoinNode extends TwoInputNode {

	private List<OperatorDescriptorDual> dataProperties;

	/**
	 * Creates a new two input node for the optimizer plan, representing the given operator.
	 *
	 * @param operator The operator that the optimizer DAG node should represent.
	 */
	public OuterJoinNode(OuterJoinOperatorBase<?, ?, ?, ?> operator) {
		super(operator);

		this.dataProperties = getDataProperties();
	}

	private List<OperatorDescriptorDual> getDataProperties() {
		OuterJoinOperatorBase<?, ?, ?, ?> operator = getOperator();

		OuterJoinType type = operator.getOuterJoinType();

		JoinHint joinHint = operator.getJoinHint();
		joinHint = joinHint == null ? JoinHint.OPTIMIZER_CHOOSES : joinHint;

		List<OperatorDescriptorDual> list = new ArrayList<>();
		switch (joinHint) {
			case OPTIMIZER_CHOOSES:
				list.add(getSortMergeDescriptor(type, true));
				break;
			case REPARTITION_SORT_MERGE:
				list.add(getSortMergeDescriptor(type, false));
				break;
			case REPARTITION_HASH_FIRST:
			case REPARTITION_HASH_SECOND:
			case BROADCAST_HASH_FIRST:
			case BROADCAST_HASH_SECOND:
			default:
				throw new CompilerException("Invalid join hint: " + joinHint + " for outer join type: " + type);
		}

		Partitioner<?> customPartitioner = operator.getCustomPartitioner();
		if (customPartitioner != null) {
			for (OperatorDescriptorDual desc : list) {
				((AbstractJoinDescriptor) desc).setCustomPartitioner(customPartitioner);
			}
		}
		return list;
	}

	private OperatorDescriptorDual getSortMergeDescriptor(OuterJoinType type, boolean broadcastAllowed) {
		if (type == OuterJoinType.FULL) {
			return new SortMergeFullOuterJoinDescriptor(this.keys1, this.keys2);
		} else if (type == OuterJoinType.LEFT) {
			return new SortMergeLeftOuterJoinDescriptor(this.keys1, this.keys2, broadcastAllowed);
		} else {
			return new SortMergeRightOuterJoinDescriptor(this.keys1, this.keys2, broadcastAllowed);
		}
	}

	@Override
	public OuterJoinOperatorBase<?, ?, ?, ?> getOperator() {
		return (OuterJoinOperatorBase<?, ?, ?, ?>) super.getOperator();
	}

	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return dataProperties;
	}

	@Override
	public String getOperatorName() {
		return "Outer Join";
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
		long card2 = getSecondPredecessorNode().getEstimatedNumRecords();

		if (card1 < 0 || card2 < 0) {
			this.estimatedNumRecords = -1;
		} else {
			this.estimatedNumRecords = Math.max(card1, card2);
		}

		if (this.estimatedNumRecords >= 0) {
			float width1 = getFirstPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
			float width2 = getSecondPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
			float width = (width1 <= 0 || width2 <= 0) ? -1 : width1 + width2;

			if (width > 0) {
				this.estimatedOutputSize = (long) (width * this.estimatedNumRecords);
			}
		}
	}
}
