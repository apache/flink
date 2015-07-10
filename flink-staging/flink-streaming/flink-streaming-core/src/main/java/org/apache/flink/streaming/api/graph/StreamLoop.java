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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

/**
 * Object for representing loops in streaming programs.
 * 
 */
public class StreamLoop {

	private Integer loopID;

	private List<StreamNode> headOperators = new ArrayList<StreamNode>();
	private List<StreamNode> tailOperators = new ArrayList<StreamNode>();
	private List<StreamPartitioner<?>> tailPartitioners = new ArrayList<StreamPartitioner<?>>();
	private List<List<String>> tailSelectedNames = new ArrayList<List<String>>();

	private boolean coIteration = false;
	private TypeInformation<?> feedbackType = null;

	private long timeout;
	private boolean tailPartitioning = false;

	private List<Tuple2<StreamNode, StreamNode>> sourcesAndSinks = new ArrayList<Tuple2<StreamNode, StreamNode>>();

	public StreamLoop(Integer loopID, long timeout, TypeInformation<?> feedbackType) {
		this.loopID = loopID;
		this.timeout = timeout;
		if (feedbackType != null) {
			this.feedbackType = feedbackType;
			coIteration = true;
			tailPartitioning = true;
		}
	}

	public Integer getID() {
		return loopID;
	}

	public long getTimeout() {
		return timeout;
	}

	public boolean isCoIteration() {
		return coIteration;
	}

	public TypeInformation<?> getFeedbackType() {
		return feedbackType;
	}

	public void addSourceSinkPair(StreamNode source, StreamNode sink) {
		this.sourcesAndSinks.add(new Tuple2<StreamNode, StreamNode>(source, sink));
	}

	public List<Tuple2<StreamNode, StreamNode>> getSourceSinkPairs() {
		return this.sourcesAndSinks;
	}

	public void addHeadOperator(StreamNode head) {
		this.headOperators.add(head);
	}

	public void addTailOperator(StreamNode tail, StreamPartitioner<?> partitioner,
			List<String> selectedNames) {
		this.tailOperators.add(tail);
		this.tailPartitioners.add(partitioner);
		this.tailSelectedNames.add(selectedNames);
	}

	public void applyTailPartitioning() {
		this.tailPartitioning = true;
	}

	public boolean keepsPartitioning() {
		return tailPartitioning;
	}

	public List<StreamNode> getHeads() {
		return headOperators;
	}

	public List<StreamNode> getTails() {
		return tailOperators;
	}

	public List<StreamPartitioner<?>> getTailPartitioners() {
		return tailPartitioners;
	}

	public List<List<String>> getTailSelectedNames() {
		return tailSelectedNames;
	}

	@Override
	public String toString() {
		return "ID: " + loopID + "\n" + "Head: " + headOperators + "\n" + "Tail: " + tailOperators
				+ "\n" + "TP: " + tailPartitioners + "\n" + "TSN: " + tailSelectedNames;
	}
}
