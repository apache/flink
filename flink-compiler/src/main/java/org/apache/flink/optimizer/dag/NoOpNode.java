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

import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.NoOpDescriptor;

/**
 * The optimizer's internal representation of a <i>No Operation</i> node.
 */
public class NoOpNode extends UnaryOperatorNode {
	
	public NoOpNode() {
		super("No Op", new FieldSet(), new NoOpDescriptor());
	}
	
	public NoOpNode(String name) {
		super(name, new FieldSet(), new NoOpDescriptor());
	}
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
	}
}
