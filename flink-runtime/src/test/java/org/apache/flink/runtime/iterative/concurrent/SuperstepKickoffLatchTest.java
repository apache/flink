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

package org.apache.flink.runtime.iterative.concurrent;

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link SuperstepKickoffLatch}. */
public class SuperstepKickoffLatchTest {

    @Test
    public void testWaitFromOne() {
        try {
            SuperstepKickoffLatch latch = new SuperstepKickoffLatch();

            Waiter w = new Waiter(latch, 2);
            Thread waiter = new Thread(w);
            waiter.setDaemon(true);
            waiter.start();

            WatchDog wd = new WatchDog(waiter, 2000);
            wd.start();

            Thread.sleep(100);

            latch.triggerNextSuperstep();

            wd.join();
            if (wd.getError() != null) {
                throw wd.getError();
            }

            if (w.getError() != null) {
                throw w.getError();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Error: " + t.getMessage());
        }
    }

    @Test
    public void testWaitAlreadyFulfilled() {
        try {
            SuperstepKickoffLatch latch = new SuperstepKickoffLatch();
            latch.triggerNextSuperstep();

            Waiter w = new Waiter(latch, 2);
            Thread waiter = new Thread(w);
            waiter.setDaemon(true);
            waiter.start();

            WatchDog wd = new WatchDog(waiter, 2000);
            wd.start();

            Thread.sleep(100);

            wd.join();
            if (wd.getError() != null) {
                throw wd.getError();
            }

            if (w.getError() != null) {
                throw w.getError();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Error: " + t.getMessage());
        }
    }

    @Test
    public void testWaitIncorrect() {
        try {
            SuperstepKickoffLatch latch = new SuperstepKickoffLatch();
            latch.triggerNextSuperstep();
            latch.triggerNextSuperstep();

            try {
                latch.awaitStartOfSuperstepOrTermination(2);
                Assert.fail("should throw exception");
            } catch (IllegalStateException e) {
                // good
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Error: " + e.getMessage());
        }
    }

    @Test
    public void testWaitIncorrectAsync() {
        try {
            SuperstepKickoffLatch latch = new SuperstepKickoffLatch();
            latch.triggerNextSuperstep();
            latch.triggerNextSuperstep();

            Waiter w = new Waiter(latch, 2);
            Thread waiter = new Thread(w);
            waiter.setDaemon(true);
            waiter.start();

            WatchDog wd = new WatchDog(waiter, 2000);
            wd.start();

            Thread.sleep(100);

            wd.join();
            if (wd.getError() != null) {
                throw wd.getError();
            }

            if (w.getError() != null) {
                if (!(w.getError() instanceof IllegalStateException)) {
                    throw new Exception("wrong exception type " + w.getError());
                }
            } else {
                Assert.fail("should cause exception");
            }
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Error: " + t.getMessage());
        }
    }

    @Test
    public void testWaitForTermination() {
        try {
            SuperstepKickoffLatch latch = new SuperstepKickoffLatch();
            latch.triggerNextSuperstep();
            latch.triggerNextSuperstep();

            Waiter w = new Waiter(latch, 4);
            Thread waiter = new Thread(w);
            waiter.setDaemon(true);
            waiter.start();

            WatchDog wd = new WatchDog(waiter, 2000);
            wd.start();

            latch.signalTermination();

            wd.join();
            if (wd.getError() != null) {
                throw wd.getError();
            }

            if (w.getError() != null) {
                throw w.getError();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.fail("Error: " + t.getMessage());
        }
    }

    private static class Waiter implements Runnable {

        private final SuperstepKickoffLatch latch;

        private final int waitFor;

        private volatile Throwable error;

        public Waiter(SuperstepKickoffLatch latch, int waitFor) {
            this.latch = latch;
            this.waitFor = waitFor;
        }

        @Override
        public void run() {
            try {
                latch.awaitStartOfSuperstepOrTermination(waitFor);
            } catch (Throwable t) {
                this.error = t;
            }
        }

        public Throwable getError() {
            return error;
        }
    }

    private static class WatchDog extends Thread {

        private final Thread toWatch;

        private final long timeOut;

        private volatile Throwable failed;

        public WatchDog(Thread toWatch, long timeout) {
            setDaemon(true);
            setName("Watchdog");
            this.toWatch = toWatch;
            this.timeOut = timeout;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void run() {
            try {
                toWatch.join(timeOut);

                if (toWatch.isAlive()) {
                    this.failed = new Exception("timed out");
                    toWatch.interrupt();

                    toWatch.join(2000);
                    if (toWatch.isAlive()) {
                        toWatch.stop();
                    }
                }
            } catch (Throwable t) {
                failed = t;
            }
        }

        public Throwable getError() {
            return failed;
        }
    }
}
