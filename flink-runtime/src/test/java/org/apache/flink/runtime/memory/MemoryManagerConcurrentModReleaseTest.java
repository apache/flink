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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;

import org.junit.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import static org.junit.Assert.fail;

/**
 * Validate memory release under concurrent modification exceptions.
 */
public class MemoryManagerConcurrentModReleaseTest {

	@Test
	public void testConcurrentModificationOnce() {
		try {
			final int numSegments = 10000;
			final int segmentSize = 4096;

			MemoryManager memMan = MemoryManagerBuilder
				.newBuilder()
				.setMemorySize(numSegments * segmentSize)
				.setPageSize(segmentSize)
				.build();

			ArrayList<MemorySegment> segs = new ListWithConcModExceptionOnFirstAccess<>();
			memMan.allocatePages(this, segs, numSegments);

			memMan.release(segs);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testConcurrentModificationWhileReleasing() {
		try {
			final int numSegments = 10000;
			final int segmentSize = 4096;

			MemoryManager memMan = MemoryManagerBuilder
				.newBuilder()
				.setMemorySize(numSegments * segmentSize)
				.setPageSize(segmentSize)
				.build();

			ArrayList<MemorySegment> segs = new ArrayList<>(numSegments);
			memMan.allocatePages(this, segs, numSegments);

			// start a thread that performs concurrent modifications
			Modifier mod = new Modifier(segs);
			Thread modRunner = new Thread(mod);
			modRunner.start();

			// give the thread some time to start working
			Thread.sleep(500);

			try {
				memMan.release(segs);
			}
			finally {
				mod.cancel();
			}

			modRunner.join();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private class Modifier implements Runnable {

		private final ArrayList<MemorySegment> toModify;

		private volatile boolean running = true;

		private Modifier(ArrayList<MemorySegment> toModify) {
			this.toModify = toModify;
		}

		public void cancel() {
			running = false;
		}

		@Override
		public void run() {
			while (running) {
				try {
					MemorySegment seg = toModify.remove(0);
					toModify.add(seg);
				}
				catch (IndexOutOfBoundsException e) {
					// may happen, just retry
				}
			}
		}
	}

	private class ListWithConcModExceptionOnFirstAccess<E> extends ArrayList<E> {

		private static final long serialVersionUID = -1623249699823349781L;

		private boolean returnedIterator;

		@Override
		public Iterator<E> iterator() {
			if (returnedIterator) {
				return super.iterator();
			}
			else {
				returnedIterator = true;
				return new ConcFailingIterator<>();
			}
		}
	}

	private class ConcFailingIterator<E> implements Iterator<E> {

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public E next() {
			throw new ConcurrentModificationException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
