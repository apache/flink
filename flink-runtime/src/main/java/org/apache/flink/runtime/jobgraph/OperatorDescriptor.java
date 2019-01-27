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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.metrics.dump.MetricQueryService;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.metrics.groups.TaskMetricGroup.METRICS_OPERATOR_NAME_MAX_LENGTH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Operator descriptor.
 */
public class OperatorDescriptor implements Serializable {

	private final String operatorName;

	private final OperatorID operatorID;

	private final String operatorMetricsName;

	private final List<OperatorEdgeDescriptor> inputs = new ArrayList<>();

	/**
	 * Instantiates a new Operator descriptor.
	 *
	 * @param operatorName the operator name
	 * @param operatorID   the operator id
	 */
	public OperatorDescriptor(String operatorName, OperatorID operatorID) {
		this.operatorName = checkNotNull(operatorName);
		this.operatorID = checkNotNull(operatorID);
		this.operatorMetricsName = name2MetricName(operatorName);
	}

	private String name2MetricName(String name){
		String metricName = name;
		if (name != null && name.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
			metricName = name.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
		}
		metricName = MetricQueryService.FILTER.filterCharacters(metricName);
		return metricName;
	}

	/**
	 * Add input edge of operator.
	 *
	 * @param operatorEdgeDescriptor the operator edge descriptor
	 */
	public void addInput(OperatorEdgeDescriptor operatorEdgeDescriptor) {
		inputs.add(operatorEdgeDescriptor);
	}

	/**
	 * Gets operator name.
	 *
	 * @return the operator name
	 */
	public String getOperatorName() {
		return operatorName;
	}

	/**
	 * Gets operator id.
	 *
	 * @return the operator id
	 */
	public OperatorID getOperatorID() {
		return operatorID;
	}

	/**
	 * Gets operator metric name.
	 * @return the operator metric name
	 */
	public String getOperatorMetricsName() {
		return operatorMetricsName;
	}

	/**
	 * Gets inputs.
	 *
	 * @return the inputs
	 */
	public List<OperatorEdgeDescriptor> getInputs() {
		return inputs;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).
			append("operatorName", operatorName).
			append("operatorID", operatorID).
			append("inputs", inputs).
			append("metricName", operatorMetricsName).
			toString();
	}
}
