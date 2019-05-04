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

package org.apache.flink.modelserving.java.model;

import org.apache.flink.model.Modeldescriptor;

/**
 * Representation of the model serving statistics.
 */
public class ModelToServeStats {

	// Model name
	private String name;
	// Model description
	private String description;
	// Model type
	private Modeldescriptor.ModelDescriptor.ModelType modelType;
	// Model usage start time
	private long since;
	// Usage number
	private long invocations;
	// Cumulative service time
	private double duration;
	// Min duration of service
	private long min;
	// Max service duration
	private long max;

	/**
	 * Default Model to serve statistics constructor.
	 *
	 */
	public ModelToServeStats(){}

	/**
	 * Create Model to serve statistics.
	 *
	 * @param name Model name.
	 * @param description Model description.
	 * @param modelType Type of the model.
	 */
	public ModelToServeStats(final String name, final String description, Modeldescriptor.ModelDescriptor.ModelType modelType) {
		this.name = name;
		this.description = description;
		this.modelType = modelType;
		this.since = 0;
		this.invocations = 0;
		this.duration = 0.;
		this.min = Long.MAX_VALUE;
		this.max = Long.MIN_VALUE;
	}

	/**
	 * Create Model to serve statistics.
	 *
	 * @param name Model name.
	 * @param description Model description.
	 * @param modelType Type of the model.
	 * @param since model deployment in Unix time.
	 * @param invocations Number of model invocations.
	 * @param duration overall model serving time across all invocations.
	 * @param min minimal model serving time.
	 * @param max maximum model serving time.
	 */
	public ModelToServeStats(final String name, final String description, Modeldescriptor.ModelDescriptor.ModelType modelType, final long since, final long invocations, final double duration, final long min, final long max) {
		this.name = name;
		this.description = description;
		this.modelType = modelType;
		this.since = since;
		this.invocations = invocations;
		this.duration = duration;
		this.min = min;
		this.max = max;
	}

	/**
	 * Create Model to serve statistics.
	 *
	 * @param model Internal generic representation for model to serve.
	 */
	public ModelToServeStats(ModelToServe model){
		this.name = model.getName();
		this.description = model.getDescription();
		this.modelType = model.getModelType();
		this.since = System.currentTimeMillis();
		this.invocations = 0;
		this.duration = 0.;
		this.min = Long.MAX_VALUE;
		this.max = Long.MIN_VALUE;
	}

	/**
	 * Increment usage. Invoked every time serving is completed.
	 *
	 * @param execution Model serving time.
	 * @return updated statistics.
	 */
	public ModelToServeStats incrementUsage(long execution){
		invocations++;
		duration += execution;
		if (execution < min) {
			min = execution;
		}
		if (execution > max) {
			max = execution;
		}
		return this;
	}

	/**
	 * Get model's name.
	 *
	 * @return model's name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set model's name.
	 *
	 * @param name model's name.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get model's description.
	 *
	 * @return model's description.
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Set model's description.
	 *
	 * @param description model's description.
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Get model's deployment time.
	 *
	 * @return model's deployment time.
	 */
	public long getSince() {
		return since;
	}

	/**
	 * Set model's deployment time.
	 *
	 * @param since model's deployment time.
	 */
	public void setSince(long since) {
		this.since = since;
	}

	/**
	 * Get number of model's invocations.
	 *
	 * @return number of model's invocations.
	 */
	public long getInvocations() {
		return invocations;
	}

	/**
	 * Set model's number of invocations.
	 *
	 * @param invocations model's number of invocations.
	 */
	public void setInvocations(long invocations) {
		this.invocations = invocations;
	}

	/**
	 * Get overall model's invocation duration.
	 *
	 * @return number overall model's invocation duration.
	 */
	public double getDuration() {
		return duration;
	}

	/**
	 * Set overall model's invocation duration.
	 *
	 * @param duration overall model's invocation duration.
	 */
	public void setDuration(double duration) {
		this.duration = duration;
	}

	/**
	 * Get min model's invocation duration.
	 *
	 * @return min model's invocation duration.
	 */
	public long getMin() {
		return min;
	}

	/**
	 * Set min model's invocation duration.
	 *
	 * @param min min model's invocation duration.
	 */
	public void setMin(long min) {
		this.min = min;
	}

	/**
	 * Get max model's invocation duration.
	 *
	 * @return max model's invocation duration.
	 */
	public long getMax() {
		return max;
	}

	/**
	 * Set max model's invocation duration.
	 *
	 * @param max max model's invocation duration.
	 */
	public void setMax(long max) {
		this.max = max;
	}

	/**
	 * Get model execution statistics as a String.
	 *
	 * @return model execution statistics as a String.
	 */
	@Override
	public String toString() {
		return "ModelServingInfo{" +
			"name='" + name + '\'' +
			", description='" + description + '\'' +
			", since=" + since +
			", invocations=" + invocations +
			", duration=" + duration +
			", min=" + min +
			", max=" + max +
			'}';
	}
}
