/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapper;
import org.apache.flink.streaming.api.collector.selector.OutputSelectorWrapperFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;

/**
 * Class representing the operators in the streaming programs, with all their
 * properties.
 * 
 */
public class StreamNode implements Serializable {

	private static final long serialVersionUID = 1L;
	private static int currentSlotSharingIndex = 1;

	transient private StreamExecutionEnvironment env;

	private Integer ID;
	private Integer parallelism = null;
	private Long bufferTimeout = null;
	private String operatorName;
	private Integer slotSharingID;
	private boolean isolatedSlot = false;

	private transient StreamOperator<?> operator;
	private List<OutputSelector<?>> outputSelectors;
	private StreamRecordSerializer<?> typeSerializerIn1;
	private StreamRecordSerializer<?> typeSerializerIn2;
	private StreamRecordSerializer<?> typeSerializerOut;

	private List<StreamEdge> inEdges = new ArrayList<StreamEdge>();
	private List<StreamEdge> outEdges = new ArrayList<StreamEdge>();

	private Class<? extends AbstractInvokable> jobVertexClass;

	private InputFormat<?, ?> inputFormat;

	public StreamNode(StreamExecutionEnvironment env, Integer ID, StreamOperator<?> operator,
			String operatorName, List<OutputSelector<?>> outputSelector,
			Class<? extends AbstractInvokable> jobVertexClass) {
		this.env = env;
		this.ID = ID;
		this.operatorName = operatorName;
		this.operator = operator;
		this.outputSelectors = outputSelector;
		this.jobVertexClass = jobVertexClass;
		this.slotSharingID = currentSlotSharingIndex;
	}

	public void addInEdge(StreamEdge inEdge) {
		if (inEdge.getTargetID() != getID()) {
			throw new IllegalArgumentException("Destination ID doesn't match the StreamNode ID");
		} else {
			inEdges.add(inEdge);
		}
	}

	public void addOutEdge(StreamEdge outEdge) {
		if (outEdge.getSourceID() != getID()) {
			throw new IllegalArgumentException("Source ID doesn't match the StreamNode ID");
		} else {
			outEdges.add(outEdge);
		}
	}

	public List<StreamEdge> getOutEdges() {
		return outEdges;
	}

	public List<StreamEdge> getInEdges() {
		return inEdges;
	}

	public List<Integer> getOutEdgeIndices() {
		List<Integer> outEdgeIndices = new ArrayList<Integer>();

		for (StreamEdge edge : outEdges) {
			outEdgeIndices.add(edge.getTargetID());
		}

		return outEdgeIndices;
	}

	public List<Integer> getInEdgeIndices() {
		List<Integer> inEdgeIndices = new ArrayList<Integer>();

		for (StreamEdge edge : inEdges) {
			inEdgeIndices.add(edge.getSourceID());
		}

		return inEdgeIndices;
	}

	public Integer getID() {
		return ID;
	}

	public int getParallelism() {
		return parallelism != null ? parallelism : env.getParallelism();
	}

	public void setParallelism(Integer parallelism) {
		this.parallelism = parallelism;
	}

	public Long getBufferTimeout() {
		return bufferTimeout != null ? bufferTimeout : env.getBufferTimeout();
	}

	public void setBufferTimeout(Long bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	public StreamOperator<?> getOperator() {
		return operator;
	}

	public void setOperator(StreamOperator<?> operator) {
		this.operator = operator;
	}

	public String getOperatorName() {
		return operatorName;
	}

	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}

	public List<OutputSelector<?>> getOutputSelectors() {
		return outputSelectors;
	}

	public OutputSelectorWrapper<?> getOutputSelectorWrapper() {
		return OutputSelectorWrapperFactory.create(getOutputSelectors());
	}

	public void addOutputSelector(OutputSelector<?> outputSelector) {
		this.outputSelectors.add(outputSelector);
	}

	public StreamRecordSerializer<?> getTypeSerializerIn1() {
		return typeSerializerIn1;
	}

	public void setSerializerIn1(StreamRecordSerializer<?> typeSerializerIn1) {
		this.typeSerializerIn1 = typeSerializerIn1;
	}

	public StreamRecordSerializer<?> getTypeSerializerIn2() {
		return typeSerializerIn2;
	}

	public void setSerializerIn2(StreamRecordSerializer<?> typeSerializerIn2) {
		this.typeSerializerIn2 = typeSerializerIn2;
	}

	public StreamRecordSerializer<?> getTypeSerializerOut() {
		return typeSerializerOut;
	}

	public void setSerializerOut(StreamRecordSerializer<?> typeSerializerOut) {
		this.typeSerializerOut = typeSerializerOut;
	}

	public Class<? extends AbstractInvokable> getJobVertexClass() {
		return jobVertexClass;
	}

	public InputFormat<?, ?> getInputFormat() {
		return inputFormat;
	}

	public void setInputFormat(InputFormat<?, ?> inputFormat) {
		this.inputFormat = inputFormat;
	}

	public int getSlotSharingID() {
		return isolatedSlot ? -1 : slotSharingID;
	}

	public void startNewSlotSharingGroup() {
		this.slotSharingID = ++currentSlotSharingIndex;
	}

	public void isolateSlot() {
		isolatedSlot = true;
	}
	
	@Override
	public String toString() {
		return operatorName + ID;
	}

}
