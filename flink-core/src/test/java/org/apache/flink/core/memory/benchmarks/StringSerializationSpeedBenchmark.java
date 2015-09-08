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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

public class StringSerializationSpeedBenchmark {
	
	public static void main(String[] args) throws Exception {
		
		final int LARGE_SEGMENT_SIZE = 1024 * 1024 * 1024;

		final byte[] largeSegment = new byte[LARGE_SEGMENT_SIZE];

		final ByteBuffer largeOffHeap = ByteBuffer.allocateDirect(LARGE_SEGMENT_SIZE);

		final String[] randomStrings = generateRandomStrings(5468917685263896L, 1000, 128, 6, true);

		final StringSerializer ser = StringSerializer.INSTANCE;
		
		final int outerRounds = 10;
		final int innerRounds = 5000;

		{
			System.out.println("testing core heap memory segment");

			long start = System.nanoTime();
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
			long stop = System.nanoTime();

			System.out.println(String.format("Core heap memory segment took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing core hybrid memory segment on heap");

			long start = System.nanoTime();
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
			long stop = System.nanoTime();

			System.out.println(String.format("Core hybrid memory segment on heap took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing core hybrid memory segment off heap");

			long start = System.nanoTime();
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
			long stop = System.nanoTime();

			System.out.println(String.format("Core hybrid memory segment off heap took %,d msecs", (stop - start) / 1000000));
		}
		
		{
			System.out.println("testing pure hybrid memory segment on heap");

			long start = System.nanoTime();
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
			long stop = System.nanoTime();

			System.out.println(String.format("Pure hybrid on heap memory segment took %,d msecs", (stop - start) / 1000000));
		}

		{
			System.out.println("testing pure hybrid memory segment off heap");

			long start = System.nanoTime();
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
			long stop = System.nanoTime();

			System.out.println(String.format("Pure hybrid off heap memory segment took %,d msecs", (stop - start) / 1000000));
		}
		
		{
			System.out.println("testing pure heap memory segment");

			long start = System.nanoTime();
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
			long stop = System.nanoTime();

			System.out.println(String.format("Pure heap memory segment took %,d msecs", (stop - start) / 1000000));
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
}
