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

package org.apache.flink.table.util.resource;

import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;

/**
 * StreamEdge Properties.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamEdgeProperty extends AbstractJsonSerializable implements Comparable<StreamEdgeProperty> {
	private String source;
	private String target;
	private int index = 0;

	public static final Set<String> STRATEGY_UPDATABLE = new HashSet<>();
	public static final String FORWARD_STRATEGY = "FORWARD";
	public static final String REBALANCE_STRATEGY = "REBALANCE";
	public static final String RESCALE_STRATEGY = "RESCALE";
	static {
		STRATEGY_UPDATABLE.add(FORWARD_STRATEGY);
		STRATEGY_UPDATABLE.add(REBALANCE_STRATEGY);
		STRATEGY_UPDATABLE.add(RESCALE_STRATEGY);
	}

	@JsonProperty("ship_strategy")
	private String shipStrategy = FORWARD_STRATEGY;

	public StreamEdgeProperty() {
	}

	public StreamEdgeProperty(String source, String target, int index) {
		this.source = source;
		this.target = target;
		this.index = index;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getShipStrategy() {
		return shipStrategy;
	}

	public void setShipStrategy(String shipStrategy) {
		this.shipStrategy = shipStrategy;
	}

	@Override
	public int compareTo(StreamEdgeProperty streamEdgeProperty) {
		int r = this.source.compareTo(streamEdgeProperty.source);
		if (r == 0) {
			r = this.target.compareTo(streamEdgeProperty.target);
		}
		if (r == 0) {
			r = this.index - streamEdgeProperty.index;
		}
		return r;
	}

	public void apply(StreamEdge edge, StreamGraph graph) {
		if (shipStrategy.equalsIgnoreCase(FORWARD_STRATEGY)) {
			if (graph.getStreamNode(edge.getTargetId()).getParallelism() !=
					graph.getStreamNode(edge.getSourceId()).getParallelism()) {
				edge.setPartitioner(new RebalancePartitioner<>());
			} else {
				edge.setPartitioner(new ForwardPartitioner<>());
			}
		} else if (shipStrategy.equalsIgnoreCase(RESCALE_STRATEGY)) {
			edge.setPartitioner(new RescalePartitioner<>());
		} else if (shipStrategy.equalsIgnoreCase(REBALANCE_STRATEGY)) {
			edge.setPartitioner(new RebalancePartitioner<>());
		}
	}

	public void update(StreamEdgeProperty property) {
		if (STRATEGY_UPDATABLE.contains(this.shipStrategy.toUpperCase()) &&
				STRATEGY_UPDATABLE.contains(property.shipStrategy.toUpperCase())) {
			this.shipStrategy = property.getShipStrategy();
		} else if (!this.shipStrategy.equalsIgnoreCase(property.getShipStrategy())) {
			throw new RuntimeException("Fail to apply resource configuration file to edge " + this + ".");
		}
	}
}

