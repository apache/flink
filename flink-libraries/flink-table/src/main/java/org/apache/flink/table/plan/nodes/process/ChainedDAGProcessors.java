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

package org.apache.flink.table.plan.nodes.process;

import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.resource.batch.managedmem.BatchManagedMemoryProcessor;
import org.apache.flink.table.resource.batch.parallelism.BatchParallelismProcessor;
import org.apache.flink.table.resource.common.NodePartialResProcessor;
import org.apache.flink.table.resource.stream.StreamParallelismProcessor;

import java.util.LinkedList;
import java.util.List;

/**
 * Chained dag processors that process dag in order.
 */
public class ChainedDAGProcessors {

	public ChainedDAGProcessors() {

	}

	private List<DAGProcessor> dagProcessorList = new LinkedList<>();

	public static ChainedDAGProcessors buildBatchProcessors() {
		ChainedDAGProcessors processors = new ChainedDAGProcessors();
		processors.addProcessor(new NodePartialResProcessor());
		processors.addProcessor(new BatchParallelismProcessor());
		processors.addProcessor(new BatchManagedMemoryProcessor());
		return processors;
	}

	public static ChainedDAGProcessors buildStreamProcessors() {
		ChainedDAGProcessors processors = new ChainedDAGProcessors();
		processors.addProcessor(new NodePartialResProcessor());
		processors.addProcessor(new StreamParallelismProcessor());
		return processors;
	}

	public void addProcessor(DAGProcessor dagProcessorList) {
		this.dagProcessorList.add(dagProcessorList);
	}

	public List<ExecNode<?, ?>> process(List<ExecNode<?, ?>> sinkNodes, DAGProcessContext context) {
		for (DAGProcessor processor : dagProcessorList) {
			sinkNodes = processor.process(sinkNodes, context);
		}
		return sinkNodes;
	}
}
