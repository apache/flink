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

package org.apache.flink.api.common.interactive;

import org.apache.flink.util.AbstractID;

import java.util.Set;

/**
 * This is the default implementation of IntermediateResultSummary.
 * See {@link IntermediateResultSummary} for more details.
 */
public class DefaultIntermediateResultSummary
	implements IntermediateResultSummary<AbstractID, IntermediateResultDescriptors> {

	private final IntermediateResultDescriptors intermediateResultDescriptors;

	public DefaultIntermediateResultSummary() {
		this(new IntermediateResultDescriptors());
	}

	public DefaultIntermediateResultSummary(IntermediateResultDescriptors intermediateResultDescriptor) {
		this.intermediateResultDescriptors = intermediateResultDescriptor;
	}

	@Override
	public IntermediateResultDescriptors getIntermediateResultDescriptors() {
		return intermediateResultDescriptors;
	}

	@Override
	public Set<AbstractID> getIncompleteIntermediateDataSetIds() {
		return intermediateResultDescriptors.getIncompleteIntermediateDataSetIds();
	}

	@Override
	public void merge(IntermediateResultSummary<AbstractID, IntermediateResultDescriptors> summary) {
		intermediateResultDescriptors.getIntermediateResultDescriptors().putAll(summary.getIntermediateResultDescriptors().getIntermediateResultDescriptors());
		intermediateResultDescriptors.getIncompleteIntermediateDataSetIds().addAll(summary.getIncompleteIntermediateDataSetIds());
	}
}
