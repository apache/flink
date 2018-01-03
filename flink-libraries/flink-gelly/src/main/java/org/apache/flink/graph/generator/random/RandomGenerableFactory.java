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

package org.apache.flink.graph.generator.random;

import org.apache.commons.math3.random.RandomGenerator;

import java.util.List;

/**
 * A {@code RandomGenerableFactory} returns a scale-free collection of sources
 * of pseudorandomness which can be used to perform repeatable parallel
 * computation regardless of parallelism.
 *
 * <pre>
 * {@code
 * RandomGenerableFactory<JDKRandomGenerator> factory = new JDKRandomGeneratorFactory()
 *
 * List<BlockInfo<T>> generatorBlocks = factory
 *     .getRandomGenerables(elementCount, cyclesPerElement);
 *
 * DataSet<...> generatedEdges = env
 *     .fromCollection(generatorBlocks)
 *         .name("Random generators")
 *     .flatMap(...
 * }
 * </pre>
 *
 * @param <T> the type of the {@code RandomGenerator}
 */
public interface RandomGenerableFactory<T extends RandomGenerator> {

	/**
	 * The amount of work ({@code elementCount * cyclesPerElement}) is used to
	 * generate a list of blocks of work of near-equal size.
	 *
	 * @param elementCount number of elements, as indexed in the {@code BlockInfo}
	 * @param cyclesPerElement number of cycles of the PRNG per element
	 * @return the list of configuration blocks
	 */
	List<BlockInfo<T>> getRandomGenerables(long elementCount, int cyclesPerElement);
}
