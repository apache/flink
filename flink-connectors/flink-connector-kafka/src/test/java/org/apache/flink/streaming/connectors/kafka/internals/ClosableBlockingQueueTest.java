/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ClosableBlockingQueue}.
 */
public class ClosableBlockingQueueTest {

	// ------------------------------------------------------------------------
	//  single-threaded unit tests
	// ------------------------------------------------------------------------

	@Test
	public void testCreateQueueHashCodeEquals() {
		try {
			ClosableBlockingQueue<String> queue1 = new ClosableBlockingQueue<>();
			ClosableBlockingQueue<String> queue2 = new ClosableBlockingQueue<>(22);

			assertTrue(queue1.isOpen());
			assertTrue(queue2.isOpen());
			assertTrue(queue1.isEmpty());
			assertTrue(queue2.isEmpty());
			assertEquals(0, queue1.size());
			assertEquals(0, queue2.size());

			assertTrue(queue1.hashCode() == queue2.hashCode());
			//noinspection EqualsWithItself
			assertTrue(queue1.equals(queue1));
			//noinspection EqualsWithItself
			assertTrue(queue2.equals(queue2));
			assertTrue(queue1.equals(queue2));

			assertNotNull(queue1.toString());
			assertNotNull(queue2.toString());

			List<String> elements = new ArrayList<>();
			elements.add("a");
			elements.add("b");
			elements.add("c");

			ClosableBlockingQueue<String> queue3 = new ClosableBlockingQueue<>(elements);
			ClosableBlockingQueue<String> queue4 = new ClosableBlockingQueue<>(asList("a", "b", "c"));

			assertTrue(queue3.isOpen());
			assertTrue(queue4.isOpen());
			assertFalse(queue3.isEmpty());
			assertFalse(queue4.isEmpty());
			assertEquals(3, queue3.size());
			assertEquals(3, queue4.size());

			assertTrue(queue3.hashCode() == queue4.hashCode());
			//noinspection EqualsWithItself
			assertTrue(queue3.equals(queue3));
			//noinspection EqualsWithItself
			assertTrue(queue4.equals(queue4));
			assertTrue(queue3.equals(queue4));

			assertNotNull(queue3.toString());
			assertNotNull(queue4.toString());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCloseEmptyQueue() {
		try {
			ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();
			assertTrue(queue.isOpen());
			assertTrue(queue.close());
			assertFalse(queue.isOpen());

			assertFalse(queue.addIfOpen("element"));
			assertTrue(queue.isEmpty());

			try {
				queue.add("some element");
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCloseNonEmptyQueue() {
		try {
			ClosableBlockingQueue<Integer> queue = new ClosableBlockingQueue<>(asList(1, 2, 3));
			assertTrue(queue.isOpen());

			assertFalse(queue.close());
			assertFalse(queue.close());

			queue.poll();

			assertFalse(queue.close());
			assertFalse(queue.close());

			queue.pollBatch();

			assertTrue(queue.close());
			assertFalse(queue.isOpen());

			assertFalse(queue.addIfOpen(42));
			assertTrue(queue.isEmpty());

			try {
				queue.add(99);
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPeekAndPoll() {
		try {
			ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

			assertNull(queue.peek());
			assertNull(queue.peek());
			assertNull(queue.poll());
			assertNull(queue.poll());

			assertEquals(0, queue.size());

			queue.add("a");
			queue.add("b");
			queue.add("c");

			assertEquals(3, queue.size());

			assertEquals("a", queue.peek());
			assertEquals("a", queue.peek());
			assertEquals("a", queue.peek());

			assertEquals(3, queue.size());

			assertEquals("a", queue.poll());
			assertEquals("b", queue.poll());

			assertEquals(1, queue.size());

			assertEquals("c", queue.peek());
			assertEquals("c", queue.peek());

			assertEquals("c", queue.poll());

			assertEquals(0, queue.size());
			assertNull(queue.poll());
			assertNull(queue.peek());
			assertNull(queue.peek());

			assertTrue(queue.close());

			try {
				queue.peek();
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}

			try {
				queue.poll();
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPollBatch() {
		try {
			ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

			assertNull(queue.pollBatch());

			queue.add("a");
			queue.add("b");

			assertEquals(asList("a", "b"), queue.pollBatch());
			assertNull(queue.pollBatch());

			queue.add("c");

			assertEquals(singletonList("c"), queue.pollBatch());
			assertNull(queue.pollBatch());

			assertTrue(queue.close());

			try {
				queue.pollBatch();
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetElementBlocking() {
		try {
			ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

			assertNull(queue.getElementBlocking(1));
			assertNull(queue.getElementBlocking(3));
			assertNull(queue.getElementBlocking(2));

			assertEquals(0, queue.size());

			queue.add("a");
			queue.add("b");
			queue.add("c");
			queue.add("d");
			queue.add("e");
			queue.add("f");

			assertEquals(6, queue.size());

			assertEquals("a", queue.getElementBlocking(99));
			assertEquals("b", queue.getElementBlocking());

			assertEquals(4, queue.size());

			assertEquals("c", queue.getElementBlocking(0));
			assertEquals("d", queue.getElementBlocking(1000000));
			assertEquals("e", queue.getElementBlocking());
			assertEquals("f", queue.getElementBlocking(1786598));

			assertEquals(0, queue.size());

			assertNull(queue.getElementBlocking(1));
			assertNull(queue.getElementBlocking(3));
			assertNull(queue.getElementBlocking(2));

			assertTrue(queue.close());

			try {
				queue.getElementBlocking();
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}

			try {
				queue.getElementBlocking(1000000000L);
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetBatchBlocking() {
		try {
			ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

			assertEquals(emptyList(), queue.getBatchBlocking(1));
			assertEquals(emptyList(), queue.getBatchBlocking(3));
			assertEquals(emptyList(), queue.getBatchBlocking(2));

			queue.add("a");
			queue.add("b");

			assertEquals(asList("a", "b"), queue.getBatchBlocking(900000009));

			queue.add("c");
			queue.add("d");

			assertEquals(asList("c", "d"), queue.getBatchBlocking());

			assertEquals(emptyList(), queue.getBatchBlocking(2));

			queue.add("e");

			assertEquals(singletonList("e"), queue.getBatchBlocking(0));

			queue.add("f");

			assertEquals(singletonList("f"), queue.getBatchBlocking(1000000000));

			assertEquals(0, queue.size());

			assertEquals(emptyList(), queue.getBatchBlocking(1));
			assertEquals(emptyList(), queue.getBatchBlocking(3));
			assertEquals(emptyList(), queue.getBatchBlocking(2));

			assertTrue(queue.close());

			try {
				queue.getBatchBlocking();
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}

			try {
				queue.getBatchBlocking(1000000000L);
				fail("should cause an exception");
			} catch (IllegalStateException ignored) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  multi-threaded tests
	// ------------------------------------------------------------------------

	@Test
	public void notifyOnClose() {
		try {
			final long oneYear = 365L * 24 * 60 * 60 * 1000;

			// test "getBatchBlocking()"
			final ClosableBlockingQueue<String> queue1 = new ClosableBlockingQueue<>();
			QueueCall call1 = new QueueCall() {
				@Override
				public void call() throws Exception {
					queue1.getBatchBlocking();
				}
			};
			testCallExitsOnClose(call1, queue1);

			// test "getBatchBlocking()"
			final ClosableBlockingQueue<String> queue2 = new ClosableBlockingQueue<>();
			QueueCall call2 = new QueueCall() {
				@Override
				public void call() throws Exception {
					queue2.getBatchBlocking(oneYear);
				}
			};
			testCallExitsOnClose(call2, queue2);

			// test "getBatchBlocking()"
			final ClosableBlockingQueue<String> queue3 = new ClosableBlockingQueue<>();
			QueueCall call3 = new QueueCall() {
				@Override
				public void call() throws Exception {
					queue3.getElementBlocking();
				}
			};
			testCallExitsOnClose(call3, queue3);

			// test "getBatchBlocking()"
			final ClosableBlockingQueue<String> queue4 = new ClosableBlockingQueue<>();
			QueueCall call4 = new QueueCall() {
				@Override
				public void call() throws Exception {
					queue4.getElementBlocking(oneYear);
				}
			};
			testCallExitsOnClose(call4, queue4);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
	@Test
	public void testMultiThreadedAddGet() {
		try {
			final ClosableBlockingQueue<Integer> queue = new ClosableBlockingQueue<>();
			final AtomicReference<Throwable> pushErrorRef = new AtomicReference<>();
			final AtomicReference<Throwable> pollErrorRef = new AtomicReference<>();

			final int numElements = 2000;

			Thread pusher = new Thread("pusher") {

				@Override
				public void run() {
					try {
						final Random rnd = new Random();
						for (int i = 0; i < numElements; i++) {
							queue.add(i);

							// sleep a bit, sometimes
							int sleepTime = rnd.nextInt(3);
							if (sleepTime > 1) {
								Thread.sleep(sleepTime);
							}
						}

						while (true) {
							if (queue.close()) {
								break;
							} else {
								Thread.sleep(5);
							}
						}
					} catch (Throwable t) {
						pushErrorRef.set(t);
					}
				}
			};
			pusher.start();

			Thread poller = new Thread("poller") {

				@SuppressWarnings("InfiniteLoopStatement")
				@Override
				public void run() {
					try {
						int count = 0;

						try {
							final Random rnd = new Random();
							int nextExpected = 0;

							while (true) {
								int getMethod = count % 7;
								switch (getMethod) {
									case 0: {
										Integer next = queue.getElementBlocking(1);
										if (next != null) {
											assertEquals(nextExpected, next.intValue());
											nextExpected++;
											count++;
										}
										break;
									}
									case 1: {
										List<Integer> nextList = queue.getBatchBlocking();
										for (Integer next : nextList) {
											assertNotNull(next);
											assertEquals(nextExpected, next.intValue());
											nextExpected++;
											count++;
										}
										break;
									}
									case 2: {
										List<Integer> nextList = queue.getBatchBlocking(1);
										if (nextList != null) {
											for (Integer next : nextList) {
												assertNotNull(next);
												assertEquals(nextExpected, next.intValue());
												nextExpected++;
												count++;
											}
										}
										break;
									}
									case 3: {
										Integer next = queue.poll();
										if (next != null) {
											assertEquals(nextExpected, next.intValue());
											nextExpected++;
											count++;
										}
										break;
									}
									case 4: {
										List<Integer> nextList = queue.pollBatch();
										if (nextList != null) {
											for (Integer next : nextList) {
												assertNotNull(next);
												assertEquals(nextExpected, next.intValue());
												nextExpected++;
												count++;
											}
										}
										break;
									}
									default: {
										Integer next = queue.getElementBlocking();
										assertNotNull(next);
										assertEquals(nextExpected, next.intValue());
										nextExpected++;
										count++;
									}
								}

								// sleep a bit, sometimes
								int sleepTime = rnd.nextInt(3);
								if (sleepTime > 1) {
									Thread.sleep(sleepTime);
								}
							}
						} catch (IllegalStateException e) {
							// we get this once the queue is closed
							assertEquals(numElements, count);
						}
					} catch (Throwable t) {
						pollErrorRef.set(t);
					}
				}
			};
			poller.start();

			pusher.join();
			poller.join();

			if (pushErrorRef.get() != null) {
				Throwable t = pushErrorRef.get();
				t.printStackTrace();
				fail("Error in pusher: " + t.getMessage());
			}
			if (pollErrorRef.get() != null) {
				Throwable t = pollErrorRef.get();
				t.printStackTrace();
				fail("Error in poller: " + t.getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static void testCallExitsOnClose(
			final QueueCall call, ClosableBlockingQueue<String> queue) throws Exception {

		final AtomicReference<Throwable> errorRef = new AtomicReference<>();

		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				try {
					call.call();
				} catch (Throwable t) {
					errorRef.set(t);
				}
			}
		};

		Thread thread = new Thread(runnable);
		thread.start();
		Thread.sleep(100);
		queue.close();
		thread.join();

		@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
		Throwable cause = errorRef.get();
		assertTrue(cause instanceof IllegalStateException);
	}

	private interface QueueCall {
		void call() throws Exception;
	}
}
