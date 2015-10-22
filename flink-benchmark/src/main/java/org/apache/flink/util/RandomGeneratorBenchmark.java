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
package org.apache.flink.util;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class RandomGeneratorBenchmark {

	@BenchmarkMode(Mode.Throughput)
	@Fork(1)
	@State(Scope.Thread)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public static abstract class AbstractRandomBench {
		private final static long ITERATOR_NUMBER = 10000000;
		protected Random random;

		@Setup
		public abstract void init();

		@Benchmark
		@Warmup(iterations = 5)
		@Measurement(iterations = 5)
		public void bench() {
			for (int i = 0; i < ITERATOR_NUMBER; i++) {
				random.nextInt();
			}
		}
	}

	public static class RandomBench extends AbstractRandomBench {
		@Override
		public void init() {
			this.random = new Random(11);
		}
	}

	public static class XORShiftRandomBench extends AbstractRandomBench {

		@Override
		public void init() {
			this.random = new XORShiftRandom(11);
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(".*" + RandomGeneratorBenchmark.class.getSimpleName() +
			".*").build();
		new Runner(opt).run();
	}
}
