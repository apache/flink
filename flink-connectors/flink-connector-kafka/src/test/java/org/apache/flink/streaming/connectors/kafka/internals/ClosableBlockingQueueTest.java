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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link ClosableBlockingQueue}. */
public class ClosableBlockingQueueTest {

    // ------------------------------------------------------------------------
    //  single-threaded unit tests
    // ------------------------------------------------------------------------

    @Test
    public void testCreateQueueHashCodeEquals() {
        try {
            ClosableBlockingQueue<String> queue1 = new ClosableBlockingQueue<>();
            ClosableBlockingQueue<String> queue2 = new ClosableBlockingQueue<>(22);

            assertThat(queue1.isOpen()).isTrue();
            assertThat(queue2.isOpen()).isTrue();
            assertThat(queue1.isEmpty()).isTrue();
            assertThat(queue2.isEmpty()).isTrue();
            assertThat(queue1.size()).isEqualTo(0);
            assertThat(queue2.size()).isEqualTo(0);

            assertThat(queue1.hashCode()).isEqualTo(queue2.hashCode());
            //noinspection EqualsWithItself
            assertThat(queue1.equals(queue1)).isTrue();
            //noinspection EqualsWithItself
            assertThat(queue2.equals(queue2)).isTrue();
            assertThat(queue1.equals(queue2)).isTrue();

            assertThat(queue1.toString()).isNotNull();
            assertThat(queue2.toString()).isNotNull();

            List<String> elements = new ArrayList<>();
            elements.add("a");
            elements.add("b");
            elements.add("c");

            ClosableBlockingQueue<String> queue3 = new ClosableBlockingQueue<>(elements);
            ClosableBlockingQueue<String> queue4 =
                    new ClosableBlockingQueue<>(asList("a", "b", "c"));

            assertThat(queue3.isOpen()).isTrue();
            assertThat(queue4.isOpen()).isTrue();
            assertThat(queue3.isEmpty()).isFalse();
            assertThat(queue4.isEmpty()).isFalse();
            assertThat(queue3.size()).isEqualTo(3);
            assertThat(queue4.size()).isEqualTo(3);

            assertThat(queue3.hashCode()).isEqualTo(queue4.hashCode());
            //noinspection EqualsWithItself
            assertThat(queue3.equals(queue3)).isTrue();
            //noinspection EqualsWithItself
            assertThat(queue4.equals(queue4)).isTrue();
            assertThat(queue3.equals(queue4)).isTrue();

            assertThat(queue3.toString()).isNotNull();
            assertThat(queue4.toString()).isNotNull();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseEmptyQueue() {
        try {
            ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();
            assertThat(queue.isOpen()).isTrue();
            assertThat(queue.close()).isTrue();
            assertThat(queue.isOpen()).isFalse();

            assertThat(queue.addIfOpen("element")).isFalse();
            assertThat(queue.isEmpty()).isTrue();

            try {
                queue.add("some element");
                fail("should cause an exception");
            } catch (IllegalStateException ignored) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseNonEmptyQueue() {
        try {
            ClosableBlockingQueue<Integer> queue = new ClosableBlockingQueue<>(asList(1, 2, 3));
            assertThat(queue.isOpen()).isTrue();

            assertThat(queue.close()).isFalse();
            assertThat(queue.close()).isFalse();

            queue.poll();

            assertThat(queue.close()).isFalse();
            assertThat(queue.close()).isFalse();

            queue.pollBatch();

            assertThat(queue.close()).isTrue();
            assertThat(queue.isOpen()).isFalse();

            assertThat(queue.addIfOpen(42)).isFalse();
            assertThat(queue.isEmpty()).isTrue();

            try {
                queue.add(99);
                fail("should cause an exception");
            } catch (IllegalStateException ignored) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPeekAndPoll() {
        try {
            ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

            assertThat(queue.peek()).isNull();
            assertThat(queue.peek()).isNull();
            assertThat(queue.poll()).isNull();
            assertThat(queue.poll()).isNull();

            assertThat(queue.size()).isEqualTo(0);

            queue.add("a");
            queue.add("b");
            queue.add("c");

            assertThat(queue.size()).isEqualTo(3);

            assertThat(queue.peek()).isEqualTo("a");
            assertThat(queue.peek()).isEqualTo("a");
            assertThat(queue.peek()).isEqualTo("a");

            assertThat(queue.size()).isEqualTo(3);

            assertThat(queue.poll()).isEqualTo("a");
            assertThat(queue.poll()).isEqualTo("b");

            assertThat(queue.size()).isEqualTo(1);

            assertThat(queue.peek()).isEqualTo("c");
            assertThat(queue.peek()).isEqualTo("c");

            assertThat(queue.poll()).isEqualTo("c");

            assertThat(queue.size()).isEqualTo(0);
            assertThat(queue.poll()).isNull();
            assertThat(queue.peek()).isNull();
            assertThat(queue.peek()).isNull();

            assertThat(queue.close()).isTrue();

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
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPollBatch() {
        try {
            ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

            assertThat(queue.pollBatch()).isNull();

            queue.add("a");
            queue.add("b");

            assertThat(queue.pollBatch()).isEqualTo(asList("a", "b"));
            assertThat(queue.pollBatch()).isNull();

            queue.add("c");

            assertThat(queue.pollBatch()).containsExactly("c");
            assertThat(queue.pollBatch()).isNull();

            assertThat(queue.close()).isTrue();

            try {
                queue.pollBatch();
                fail("should cause an exception");
            } catch (IllegalStateException ignored) {
                // expected
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetElementBlocking() {
        try {
            ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

            assertThat(queue.getElementBlocking(1)).isNull();
            assertThat(queue.getElementBlocking(3)).isNull();
            assertThat(queue.getElementBlocking(2)).isNull();

            assertThat(queue.size()).isEqualTo(0);

            queue.add("a");
            queue.add("b");
            queue.add("c");
            queue.add("d");
            queue.add("e");
            queue.add("f");

            assertThat(queue.size()).isEqualTo(6);

            assertThat(queue.getElementBlocking(99)).isEqualTo("a");
            assertThat(queue.getElementBlocking()).isEqualTo("b");

            assertThat(queue.size()).isEqualTo(4);

            assertThat(queue.getElementBlocking(0)).isEqualTo("c");
            assertThat(queue.getElementBlocking(1000000)).isEqualTo("d");
            assertThat(queue.getElementBlocking()).isEqualTo("e");
            assertThat(queue.getElementBlocking(1786598)).isEqualTo("f");

            assertThat(queue.size()).isEqualTo(0);

            assertThat(queue.getElementBlocking(1)).isNull();
            assertThat(queue.getElementBlocking(3)).isNull();
            assertThat(queue.getElementBlocking(2)).isNull();

            assertThat(queue.close()).isTrue();

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
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetBatchBlocking() {
        try {
            ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>();

            assertThat(queue.getBatchBlocking(1)).isEmpty();
            assertThat(queue.getBatchBlocking(3)).isEmpty();
            assertThat(queue.getBatchBlocking(2)).isEmpty();

            queue.add("a");
            queue.add("b");

            assertThat(queue.getBatchBlocking(900000009)).isEqualTo(asList("a", "b"));

            queue.add("c");
            queue.add("d");

            assertThat(queue.getBatchBlocking()).isEqualTo(asList("c", "d"));

            assertThat(queue.getBatchBlocking(2)).isEmpty();

            queue.add("e");

            assertThat(queue.getBatchBlocking(0)).containsExactly("e");

            queue.add("f");

            assertThat(queue.getBatchBlocking(1000000000)).containsExactly("f");

            assertThat(queue.size()).isEqualTo(0);

            assertThat(queue.getBatchBlocking(1)).isEmpty();
            assertThat(queue.getBatchBlocking(3)).isEmpty();
            assertThat(queue.getBatchBlocking(2)).isEmpty();

            assertThat(queue.close()).isTrue();

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
        } catch (Exception e) {
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
            QueueCall call1 =
                    new QueueCall() {
                        @Override
                        public void call() throws Exception {
                            queue1.getBatchBlocking();
                        }
                    };
            testCallExitsOnClose(call1, queue1);

            // test "getBatchBlocking()"
            final ClosableBlockingQueue<String> queue2 = new ClosableBlockingQueue<>();
            QueueCall call2 =
                    new QueueCall() {
                        @Override
                        public void call() throws Exception {
                            queue2.getBatchBlocking(oneYear);
                        }
                    };
            testCallExitsOnClose(call2, queue2);

            // test "getBatchBlocking()"
            final ClosableBlockingQueue<String> queue3 = new ClosableBlockingQueue<>();
            QueueCall call3 =
                    new QueueCall() {
                        @Override
                        public void call() throws Exception {
                            queue3.getElementBlocking();
                        }
                    };
            testCallExitsOnClose(call3, queue3);

            // test "getBatchBlocking()"
            final ClosableBlockingQueue<String> queue4 = new ClosableBlockingQueue<>();
            QueueCall call4 =
                    new QueueCall() {
                        @Override
                        public void call() throws Exception {
                            queue4.getElementBlocking(oneYear);
                        }
                    };
            testCallExitsOnClose(call4, queue4);
        } catch (Exception e) {
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

            Thread pusher =
                    new Thread("pusher") {

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

            Thread poller =
                    new Thread("poller") {

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
                                            case 0:
                                                {
                                                    Integer next = queue.getElementBlocking(1);
                                                    if (next != null) {
                                                        assertThat(next.intValue())
                                                                .isEqualTo(nextExpected);
                                                        nextExpected++;
                                                        count++;
                                                    }
                                                    break;
                                                }
                                            case 1:
                                                {
                                                    List<Integer> nextList =
                                                            queue.getBatchBlocking();
                                                    for (Integer next : nextList) {
                                                        assertThat(next).isNotNull();
                                                        assertThat(next.intValue())
                                                                .isEqualTo(nextExpected);
                                                        nextExpected++;
                                                        count++;
                                                    }
                                                    break;
                                                }
                                            case 2:
                                                {
                                                    List<Integer> nextList =
                                                            queue.getBatchBlocking(1);
                                                    if (nextList != null) {
                                                        for (Integer next : nextList) {
                                                            assertThat(next).isNotNull();
                                                            assertThat(next.intValue())
                                                                    .isEqualTo(nextExpected);
                                                            nextExpected++;
                                                            count++;
                                                        }
                                                    }
                                                    break;
                                                }
                                            case 3:
                                                {
                                                    Integer next = queue.poll();
                                                    if (next != null) {
                                                        assertThat(next.intValue())
                                                                .isEqualTo(nextExpected);
                                                        nextExpected++;
                                                        count++;
                                                    }
                                                    break;
                                                }
                                            case 4:
                                                {
                                                    List<Integer> nextList = queue.pollBatch();
                                                    if (nextList != null) {
                                                        for (Integer next : nextList) {
                                                            assertThat(next).isNotNull();
                                                            assertThat(next.intValue())
                                                                    .isEqualTo(nextExpected);
                                                            nextExpected++;
                                                            count++;
                                                        }
                                                    }
                                                    break;
                                                }
                                            default:
                                                {
                                                    Integer next = queue.getElementBlocking();
                                                    assertThat(next).isNotNull();
                                                    assertThat(next.intValue())
                                                            .isEqualTo(nextExpected);
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
                                    assertThat(count).isEqualTo(numElements);
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
        } catch (Exception e) {
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

        Runnable runnable =
                new Runnable() {
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
        assertThat(cause).isInstanceOf(IllegalStateException.class);
    }

    private interface QueueCall {
        void call() throws Exception;
    }
}
