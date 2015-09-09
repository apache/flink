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

package org.apache.flink.core.memory.benchmarks;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

public class LongSerializationSpeedBenchmark {
	
	public static void main(String[] args) throws Exception {
		
		final int LARGE_SEGMENT_SIZE = 1024 * 1024 * 1024;
		final byte[] largeSegment = new byte[LARGE_SEGMENT_SIZE];
		final ByteBuffer largeOffHeap = ByteBuffer.allocateDirect(LARGE_SEGMENT_SIZE);

		fillOnHeap(largeSegment, (byte) -1);
		fillOffHeap(largeOffHeap, (byte) -1);
		
		final MemorySegment coreHeap = HeapMemorySegment.FACTORY.wrapPooledHeapMemory(largeSegment, null);
		final MemorySegment coreHybridOnHeap = HybridMemorySegment.FACTORY.wrapPooledHeapMemory(largeSegment, null);
		final MemorySegment coreHybridOffHeap = HybridMemorySegment.FACTORY.wrapPooledOffHeapMemory(largeOffHeap, null);
		final PureHeapMemorySegment pureHeap = new PureHeapMemorySegment(largeSegment);
		final PureHybridMemorySegment pureHybridOnHeap = new PureHybridMemorySegment(largeSegment);
		final PureHybridMemorySegment pureHybridOffHeap = new PureHybridMemorySegment(largeOffHeap);
		
		final LongSerializer ser = LongSerializer.INSTANCE;
		
		final long innerRounds = LARGE_SEGMENT_SIZE / 8;
		final int outerRounds = 10;

		{
			System.out.println("testing core heap memory segment");

			ArrayList<MemorySegment> memory = new ArrayList<>();
			memory.add(coreHeap);
			ArrayList<MemorySegment> target = new ArrayList<>();

			CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);

			long start = System.nanoTime();
			for (int outer = 0; outer < outerRounds; outer++) {
				for (long i = 0; i < innerRounds; i++) {
					ser.serialize(i, output);
				}

				target.clear();
				memory.add(coreHeap);
				output.reset();
			}
			long stop = System.nanoTime();

			System.out.println(String.format("Core heap memory segment took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing core hybrid on heap memory segment");

			ArrayList<MemorySegment> memory = new ArrayList<>();
			memory.add(coreHybridOnHeap);
			ArrayList<MemorySegment> target = new ArrayList<>();

			CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);

			long start = System.nanoTime();
			for (int outer = 0; outer < outerRounds; outer++) {
				for (long i = 0; i < innerRounds; i++) {
					ser.serialize(i, output);
				}

				target.clear();
				memory.add(coreHybridOnHeap);
				output.reset();
			}
			long stop = System.nanoTime();

			System.out.println(String.format("Core hybrid on heap memory segment took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing core hybrid off heap memory segment");

			ArrayList<MemorySegment> memory = new ArrayList<>();
			memory.add(coreHybridOffHeap);
			ArrayList<MemorySegment> target = new ArrayList<>();

			CoreMemorySegmentOutView output = new CoreMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);

			long start = System.nanoTime();
			for (int outer = 0; outer < outerRounds; outer++) {
				for (long i = 0; i < innerRounds; i++) {
					ser.serialize(i, output);
				}

				target.clear();
				memory.add(coreHybridOffHeap);
				output.reset();
			}
			long stop = System.nanoTime();

			System.out.println(String.format("Core hybrid off heap memory segment took %,d msecs", (stop - start) / 1000000));
		}
		
		{
			System.out.println("testing pure heap memory segment");

			ArrayList<PureHeapMemorySegment> memory = new ArrayList<>();
			memory.add(pureHeap);
			ArrayList<PureHeapMemorySegment> target = new ArrayList<>();

			PureHeapMemorySegmentOutView output = new PureHeapMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);
			
			long start = System.nanoTime();
			for (int outer = 0; outer < outerRounds; outer++) {
				for (long i = 0; i < innerRounds; i++) {
					ser.serialize(i, output);
				}

				target.clear();
				memory.add(pureHeap);
				output.reset();
			}
			long stop = System.nanoTime();

			System.out.println(String.format("Pure heap memory segment took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing pure hybrid memory segment on heap");

			ArrayList<PureHybridMemorySegment> memory = new ArrayList<>();
			memory.add(pureHybridOnHeap);
			ArrayList<PureHybridMemorySegment> target = new ArrayList<>();

			PureHybridMemorySegmentOutView output = new PureHybridMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);

			long start = System.nanoTime();
			for (int outer = 0; outer < outerRounds; outer++) {
				for (long i = 0; i < innerRounds; i++) {
					ser.serialize(i, output);
				}

				target.clear();
				memory.add(pureHybridOnHeap);
				output.reset();
			}
			long stop = System.nanoTime();

			System.out.println(String.format("Pure hybrid on heap memory segment took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing pure hybrid memory segment off heap");

			ArrayList<PureHybridMemorySegment> memory = new ArrayList<>();
			memory.add(pureHybridOffHeap);
			ArrayList<PureHybridMemorySegment> target = new ArrayList<>();

			PureHybridMemorySegmentOutView output = new PureHybridMemorySegmentOutView(memory, target, LARGE_SEGMENT_SIZE);

			long start = System.nanoTime();
			for (int outer = 0; outer < outerRounds; outer++) {
				for (long i = 0; i < innerRounds; i++) {
					ser.serialize(i, output);
				}

				target.clear();
				memory.add(pureHybridOffHeap);
				output.reset();
			}
			long stop = System.nanoTime();

			System.out.println(String.format("Pure hybrid off heap memory segment took %,d msecs", (stop - start) / 1000000));
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
}
