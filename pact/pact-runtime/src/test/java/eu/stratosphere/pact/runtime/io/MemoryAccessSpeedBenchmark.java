/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.io;

import java.lang.ref.WeakReference;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MemoryAccessSpeedBenchmark {
	private static final int ARRAY_LENGTH = 1024 * 1024 * 164;

	private static final int SEGMENT_OFFSET = 1024 * 1024 * 16;

	private static final int SEGMENT_LENGTH = 1024 * 1024 * 128; // 128M segment

	private static final int NUMBER_OF_ITERATIONS = 16; // x8 iterations (i.e. test for 1G read + 1G write throughput )

	private static final long RANDOM_SEED = 235646234421L;

	private static byte[] sourceBytes;

	private static byte[] targetBytes;

	private static MemorySegmentHardReference segmentHardReference;

	private static MemorySegmentWeakReference segmentWeakReference;

	private static Random random = new Random();

	@BeforeClass
	public static void initialize() {
		sourceBytes = new byte[SEGMENT_LENGTH];
		targetBytes = new byte[ARRAY_LENGTH];
		MemorySegmentDescriptor descriptor = new MemorySegmentDescriptor(targetBytes, SEGMENT_OFFSET, SEGMENT_LENGTH);
		WeakReference<MemorySegmentDescriptor> descriptorReference = new WeakReference<MemorySegmentDescriptor>(
			descriptor);
		segmentHardReference = new MemorySegmentHardReference(descriptor);
		segmentWeakReference = new MemorySegmentWeakReference(descriptorReference);
	}

	@AfterClass
	public static void destruct() {
		sourceBytes = null;
		targetBytes = null;
		segmentHardReference = null;
		segmentWeakReference = null;
	}

	@Before
	public void setUp() {
		random.setSeed(RANDOM_SEED);
		random.nextBytes(sourceBytes);
	}

	@Test
	public void testIndirectAccessWithWeakReference() {
		for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
			for (int j = 0; j < SEGMENT_LENGTH; j++) {
				segmentWeakReference.write(j, sourceBytes[j]);
			}

			for (int j = 0; j < SEGMENT_LENGTH; j++) {
				assert (sourceBytes[j] == segmentWeakReference.read(j));
			}
		}
	}

	@Test
	public void testIndirectAccessWithHardReference() {
		for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
			for (int j = 0; j < SEGMENT_LENGTH; j++) {
				segmentHardReference.write(j, sourceBytes[j]);
			}

			for (int j = 0; j < SEGMENT_LENGTH; j++) {
				assert (sourceBytes[j] == segmentHardReference.read(j));
			}
		}
	}

	@Test
	public void testDirectAccess() {
		for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
			for (int j = 0; j < SEGMENT_LENGTH; j++) {
				targetBytes[SEGMENT_OFFSET + j] = sourceBytes[j];
			}

			for (int j = 0; j < SEGMENT_LENGTH; j++) {
				assert (sourceBytes[j] == targetBytes[SEGMENT_OFFSET + j]);
			}
		}
	}

	private static final class MemorySegmentHardReference {
		private final MemorySegmentDescriptor descriptor;

		public MemorySegmentHardReference(MemorySegmentDescriptor descriptor) {
			this.descriptor = descriptor;
		}

		public byte read(int position) {
			if (position < descriptor.start || descriptor.end >= position) {
				return descriptor.memory[(position + descriptor.start)];
			} else {
				throw new IndexOutOfBoundsException();
			}
		}

		public void write(int position, byte data) {
			if (position < descriptor.start || descriptor.end >= position) {
				descriptor.memory[(position + descriptor.start)] = data;
			} else {
				throw new IndexOutOfBoundsException();
			}
		}
	}

	private static final class MemorySegmentWeakReference {
		private final WeakReference<MemorySegmentDescriptor> descriptorReference;

		public MemorySegmentWeakReference(WeakReference<MemorySegmentDescriptor> descriptorReference) {
			this.descriptorReference = descriptorReference;
		}

		public byte read(int position) {
			MemorySegmentDescriptor descriptor = descriptorReference.get();

			if (position < descriptor.start || descriptor.end >= position) {
				return descriptor.memory[(position + descriptor.start)];
			} else {
				throw new IndexOutOfBoundsException();
			}
		}

		public void write(int position, byte data) {
			MemorySegmentDescriptor descriptor = descriptorReference.get();

			if (position < descriptor.start || descriptor.end >= position) {
				descriptor.memory[(position + descriptor.start)] = data;
			} else {
				throw new IndexOutOfBoundsException();
			}
		}
	}

	private static final class MemorySegmentDescriptor {
		public final byte[] memory;

		public final int start;

		public final int end;

		public MemorySegmentDescriptor(byte[] bytes, int start, int end) {
			this.memory = bytes;
			this.start = start;
			this.end = end;
		}
	}
}
