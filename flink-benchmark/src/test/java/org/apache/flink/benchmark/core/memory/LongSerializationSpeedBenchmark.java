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

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.benchmark.core.memory.segments.CoreMemorySegmentOutView;
import org.apache.flink.benchmark.core.memory.segments.PureHeapMemorySegment;
import org.apache.flink.benchmark.core.memory.segments.PureHeapMemorySegmentOutView;
import org.apache.flink.benchmark.core.memory.segments.PureHybridMemorySegment;
import org.apache.flink.benchmark.core.memory.segments.PureHybridMemorySegmentOutView;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LongSerializationSpeedBenchmark {
	
	private final static int LARGE_SEGMENT_SIZE = 1024 * 1024 * 1024;
	
	private final byte[] largeSegment = new byte[LARGE_SEGMENT_SIZE];
	
	private final static long innerRounds = LARGE_SEGMENT_SIZE / 8;
	
	private final static int outerRounds = 10;
	
	private MemorySegment coreHeap;
	
	private MemorySegment coreHybridOnHeap;
	
	private MemorySegment coreHybridOffHeap;
	
	private PureHeapMemorySegment pureHeap;
	
	private PureHybridMemorySegment pureHybridOnHeap;
	
	private PureHybridMemorySegment pureHybridOffHeap;
	
	private LongSerializer ser;
	
	
	@Setup
	public void init() {
		final ByteBuffer largeOffHeap = ByteBuffer.allocateDirect(LARGE_SEGMENT_SIZE);
		
		fillOnHeap(largeSegment, (byte) -1);
		fillOffHeap(largeOffHeap, (byte) -1);
		
		this.coreHeap = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(largeSegment, null);
		this.coreHybridOnHeap = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(largeSegment, null);
		this.coreHybridOffHeap = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(largeOffHeap, null);
		this.pureHeap = new PureHeapMemorySegment(largeSegment);
		this.pureHybridOnHeap = new PureHybridMemorySegment(largeSegment);
		this.pureHybridOffHeap = new PureHybridMemorySegment(largeOffHeap);
		this.ser = LongSerializer.INSTANCE;
	}
	
	@Benchmark
	public void coreHeapMemorySegment() throws Exception {
		
		ArrayList<MemorySegment> memory = new ArrayList<>();
		memory.add(coreHeap);
		ArrayList<MemorySegment> target = new ArrayList<>();
		
		CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
		
		for (int outer = 0; outer < outerRounds; outer++) {
			for (long i = 0; i < innerRounds; i++) {
				ser.serialize(i, output);
			}
			
			target.clear();
			memory.add(coreHeap);
			output.reset();
		}
	}
	
	@Benchmark
	public void coreHybridOnHeapMemorySegment() throws Exception {
		
		ArrayList<MemorySegment> memory = new ArrayList<>();
		memory.add(coreHybridOnHeap);
		ArrayList<MemorySegment> target = new ArrayList<>();
		
		CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
		
		for (int outer = 0; outer < outerRounds; outer++) {
			for (long i = 0; i < innerRounds; i++) {
				ser.serialize(i, output);
			}
			
			target.clear();
			memory.add(coreHybridOnHeap);
			output.reset();
		}
	}
	
	@Benchmark
	public void coreHybridOffHeapMemorySegment() throws Exception {
		
		ArrayList<MemorySegment> memory = new ArrayList<>();
		memory.add(coreHybridOffHeap);
		ArrayList<MemorySegment> target = new ArrayList<>();
		
		CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
		
		for (int outer = 0; outer < outerRounds; outer++) {
			for (long i = 0; i < innerRounds; i++) {
				ser.serialize(i, output);
			}
			
			target.clear();
			memory.add(coreHybridOffHeap);
			output.reset();
		}
	}
	
	@Benchmark
	public void pureHeapMemorySegment() throws Exception {
		
		ArrayList<PureHeapMemorySegment> memory = new ArrayList<>();
		memory.add(pureHeap);
		ArrayList<PureHeapMemorySegment> target = new ArrayList<>();
		
		PureHeapMemorySegmentOutView output = new PureHeapMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
		
		for (int outer = 0; outer < outerRounds; outer++) {
			for (long i = 0; i < innerRounds; i++) {
				ser.serialize(i, output);
			}
			
			target.clear();
			memory.add(pureHeap);
			output.reset();
		}
	}
	
	@Benchmark
	public void pureHybridOnHeapMemorySegment() throws Exception {
		
		ArrayList<PureHybridMemorySegment> memory = new ArrayList<>();
		memory.add(pureHybridOnHeap);
		ArrayList<PureHybridMemorySegment> target = new ArrayList<>();
		
		PureHybridMemorySegmentOutView output = new PureHybridMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
		
		for (int outer = 0; outer < outerRounds; outer++) {
			for (long i = 0; i < innerRounds; i++) {
				ser.serialize(i, output);
			}
			
			target.clear();
			memory.add(pureHybridOnHeap);
			output.reset();
		}
	}
	
	@Benchmark
	public void pureHybridOffHeapMemorySegment() throws Exception {
		
		ArrayList<PureHybridMemorySegment> memory = new ArrayList<>();
		memory.add(pureHybridOffHeap);
		ArrayList<PureHybridMemorySegment> target = new ArrayList<>();
		
		PureHybridMemorySegmentOutView output = new PureHybridMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
		
		for (int outer = 0; outer < outerRounds; outer++) {
			for (long i = 0; i < innerRounds; i++) {
				ser.serialize(i, output);
			}
			
			target.clear();
			memory.add(pureHybridOffHeap);
			output.reset();
		}
	}
	
	private static void fillOnHeap(byte[] buffer, byte data) {
		for (int i = 0; i < buffer.length; i++) {
			buffer[i] = data;
		}
	}
	
	private static void fillOffHeap(ByteBuffer buffer, byte data) {
		final int len = buffer.capacity();
		for (int i = 0; i < len; i++) {
			buffer.put(i, data);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Options opt = new OptionsBuilder()
				.include(LongSerializationSpeedBenchmark.class.getSimpleName())
				.warmupIterations(2)
				.measurementIterations(2)
				.forks(1)
				.build();
		new Runner(opt).run();
	}
}
