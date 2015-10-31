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

package org.apache.flink.benchmark.core.memory;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.benchmark.core.memory.segments.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StringSerializationSpeedBenchmark {
	
	private final int LARGE_SEGMENT_SIZE = 1024 * 1024 * 1024;
	
	private final byte[] largeSegment = new byte[LARGE_SEGMENT_SIZE];
	
	private final int outerRounds = 10;
	
	private final int innerRounds = 5000;
	
	private ByteBuffer largeOffHeap;
	
	private String[] randomStrings;
	
	private StringSerializer ser;
	
	@Setup
	public void init() throws Exception {
		
		this.largeOffHeap = ByteBuffer.allocateDirect(LARGE_SEGMENT_SIZE);
		this.randomStrings = generateRandomStrings(5468917685263896L, 1000, 128, 6, true);
		this.ser = StringSerializer.INSTANCE;
		
	}
	
	@Benchmark
	public void coreHeapMemorySegment() throws Exception {
		
		for (int outer = 0; outer < outerRounds; outer++) {
			
			ArrayList<MemorySegment> memory = new ArrayList<>();
			memory.add(HeapMemorySegment.FACTORY.wrapPooledHeapMemory(largeSegment, null));
			ArrayList<MemorySegment> target = new ArrayList<>();
			
			CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			for (int i = 0; i < innerRounds; i++) {
				for (String s : randomStrings) {
					ser.serialize(s, output);
				}
			}
		}
	}
	
	@Benchmark
	public void coreHybridMemorySegmentOnHeap() throws Exception {
		
		for (int outer = 0; outer < outerRounds; outer++) {
			
			ArrayList<MemorySegment> memory = new ArrayList<>();
			memory.add(HybridMemorySegment.FACTORY.wrapPooledHeapMemory(largeSegment, null));
			ArrayList<MemorySegment> target = new ArrayList<>();
			
			CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			for (int i = 0; i < innerRounds; i++) {
				for (String s : randomStrings) {
					ser.serialize(s, output);
				}
			}
		}
	}
	
	@Benchmark
	public void coreHybridMemorySegmentOffHeap() throws Exception {
		
		for (int outer = 0; outer < outerRounds; outer++) {
			
			ArrayList<MemorySegment> memory = new ArrayList<>();
			memory.add(HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(largeOffHeap, null));
			ArrayList<MemorySegment> target = new ArrayList<>();
			
			CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			for (int i = 0; i < innerRounds; i++) {
				for (String s : randomStrings) {
					ser.serialize(s, output);
				}
			}
		}
	}
	
	@Benchmark
	public void pureHybridMemorySegmentOnheap() throws Exception {
		
		for (int outer = 0; outer < outerRounds; outer++) {
			
			ArrayList<PureHybridMemorySegment> memory = new ArrayList<>();
			memory.add(new PureHybridMemorySegment(largeSegment));
			ArrayList<PureHybridMemorySegment> target = new ArrayList<>();
			
			PureHybridMemorySegmentOutView output = new PureHybridMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			for (int i = 0; i < innerRounds; i++) {
				for (String s : randomStrings) {
					ser.serialize(s, output);
				}
			}
		}
	}
	
	@Benchmark
	public void pureHybridMemorySegmentOffHeap() throws Exception {
		
		for (int outer = 0; outer < outerRounds; outer++) {
			
			ArrayList<PureHybridMemorySegment> memory = new ArrayList<>();
			memory.add(new PureHybridMemorySegment(largeOffHeap));
			ArrayList<PureHybridMemorySegment> target = new ArrayList<>();
			
			PureHybridMemorySegmentOutView output = new PureHybridMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			for (int i = 0; i < innerRounds; i++) {
				for (String s : randomStrings) {
					ser.serialize(s, output);
				}
			}
		}
	}
	
	@Benchmark
	public void pureHeapMemorySegment() throws Exception {
		
		for (int outer = 0; outer < outerRounds; outer++) {
			
			ArrayList<PureHeapMemorySegment> memory = new ArrayList<>();
			memory.add(new PureHeapMemorySegment(largeSegment));
			ArrayList<PureHeapMemorySegment> target = new ArrayList<>();
			
			PureHeapMemorySegmentOutView output = new PureHeapMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			for (int i = 0; i < innerRounds; i++) {
				for (String s : randomStrings) {
					ser.serialize(s, output);
				}
			}
		}
	}
	
	private static String[] generateRandomStrings(long seed, int num, int maxLen, int minLen, boolean asciiOnly) {
		Random rnd = new Random(seed);
		String[] array = new String[num];
		StringBuilder bld = new StringBuilder(maxLen);
		
		int minCharValue = 40;
		int charRange = asciiOnly ? 60 : 30000;
		
		for (int i = 0; i < num; i++) {
			bld.setLength(0);
			int len = rnd.nextInt(maxLen - minLen) + minLen;
			
			for (int k = 0; k < len; k++) {
				bld.append((char) (rnd.nextInt(charRange) + minCharValue));
			}
			
			array[i] = bld.toString();
		}
		
		return array;
	}
	
	public static void main(String[] args) throws Exception {
		Options opt = new OptionsBuilder()
				.include(StringSerializationSpeedBenchmark.class.getSimpleName())
				.warmupIterations(2)
				.measurementIterations(2)
				.forks(1)
				.build();
		new Runner(opt).run();
	}
}
