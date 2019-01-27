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

package org.apache.flink.table.runtime.range;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Sample the records.
 */
@Internal
public class ReservoirSamplerWithoutReplacement implements Serializable {

	private final int numSamples;
	private final Random random;
	private IntermediateSampleData<BaseRow> smallest = null;
	private final PriorityQueue<IntermediateSampleData<BaseRow>> queue;
	private int index = 0;
	private Projection<BaseRow, BaseRow> projection;

	/**
	 * Create a new sampler with reservoir size and a supplied random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 */
	ReservoirSamplerWithoutReplacement(int numSamples, long seed) {
		Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
		this.numSamples = numSamples;
		this.random = new XORShiftRandom(seed);
		this.queue = new PriorityQueue<>(numSamples);
	}

	public void setProjection(Projection projection) {
		this.projection = projection;
	}

	void collectPartitionData(BaseRow baseRow) {
		double weight = random.nextDouble();
		if (index < numSamples) {
			// Fill the queue with first K elements from input.
			addQueue(weight, projection.apply(baseRow));
			smallest = queue.peek();
		} else {
			// Remove the element with the smallest weight,
			// and append current element into the queue.
			if (weight > smallest.getWeight()) {
				queue.remove();
				addQueue(weight, projection.apply(baseRow));
				smallest = queue.peek();
			}
		}
		index++;
	}

	void collectSampleData(IntermediateSampleData<BaseRow> sampleData) {
		if (index < numSamples) {
			// Fill the queue with first K elements from input.
			addQueue(sampleData.getWeight(), projection.apply(sampleData.getElement()));
			smallest = queue.peek();
		} else {
			// Remove the element with the smallest weight,
			// and append current element into the queue.
			if (sampleData.getWeight() > smallest.getWeight()) {
				queue.remove();
				addQueue(sampleData.getWeight(), projection.apply(sampleData.getElement()));
				smallest = queue.peek();
			}
		}
		index++;
	}

	private void addQueue(double weight, BaseRow row) {
		queue.add(new IntermediateSampleData<>(weight, row));
	}

	Iterator<IntermediateSampleData<BaseRow>> sample() {
		return queue.iterator();
	}
}
