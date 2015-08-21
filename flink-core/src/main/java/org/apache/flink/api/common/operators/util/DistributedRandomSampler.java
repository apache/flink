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
package org.apache.flink.api.common.operators.util;

import java.util.Iterator;

/**
 * For the sample with fraction, the sample algorithms are natively distributed, while it's not
 * true for fixed size sample algorithms. The fixed size sample algorithms require two-phases
 * sampling(according to our current implementation), the first phase sampling is implemented
 * in each distributed partitions, and the second phase sampling is handled in a central coordinator,
 * all the outputs of first phase sampling would feed as the input the the second phase central
 * coordinator.
 *
 * @param <T> The input data type.
 */
public abstract class DistributedRandomSampler<T> extends RandomSampler<T> {

	protected final Iterator<IntermediateSampleData<T>> EMPTY_INTERMEDIATE_ITERABLE =
		new SampledIterator<IntermediateSampleData<T>>() {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public IntermediateSampleData<T> next() {
				return null;
			}
		};

	/**
	 * Sample algorithm in the first phase, it should be executed in each distributed partitions.
	 *
	 * @param input The DataSet input of each partition.
	 * @return Intermediate sample output which would be used as the input of next phase.
	 */
	public abstract Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input);

	/**
	 * Sample algorithm in the second phase, it should be executed in the singleton coordinator.
	 *
	 * @param input All the intermediate sample output generated in the first phase.
	 * @return The sampled output.
	 */
	public abstract Iterator<T> sampleInCoordinator(Iterator<IntermediateSampleData<T>> input);

	/**
	 * Combine the first phase and second phase in sequence, implemented for test purpose only.
	 *
	 * @param input Source data.
	 * @return Sample result in sequence.
	 */
	@Override
	public Iterator<T> sample(Iterator<T> input) {
		return sampleInCoordinator(sampleInPartition(input));
	}
}
