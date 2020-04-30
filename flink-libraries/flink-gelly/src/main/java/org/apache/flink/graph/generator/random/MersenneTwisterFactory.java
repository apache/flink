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

import org.apache.commons.math3.random.MersenneTwister;

/**
 * Uses a seeded {@link MersenneTwister} to generate seeds for the
 * distributed collection of {@link MersenneTwister}.
 */
public class MersenneTwisterFactory
extends GeneratorFactoryBase<MersenneTwister> {

	public static final long DEFAULT_SEED = 0x74c8cc8a58a9ceb9L;

	public static final int MINIMUM_CYCLES_PER_BLOCK = 1 << 20;

	private final MersenneTwister random = new MersenneTwister();

	public MersenneTwisterFactory() {
		this(DEFAULT_SEED);
	}

	public MersenneTwisterFactory(long seed) {
		random.setSeed(seed);
	}

	@Override
	protected int getMinimumCyclesPerBlock() {
		return MINIMUM_CYCLES_PER_BLOCK;
	}

	@Override
	protected MersenneTwisterGenerable next() {
		return new MersenneTwisterGenerable(random.nextLong());
	}

	private static class MersenneTwisterGenerable
	implements RandomGenerable<MersenneTwister> {

		private final long seed;

		public MersenneTwisterGenerable(long seed) {
			this.seed = seed;
		}

		@Override
		public MersenneTwister generator() {
			MersenneTwister random = new MersenneTwister();

			random.setSeed(seed);

			return random;
		}
	}
}
