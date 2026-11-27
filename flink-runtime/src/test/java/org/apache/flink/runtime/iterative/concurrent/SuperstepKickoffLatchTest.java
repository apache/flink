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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SuperstepKickoffLatch}. */
class SuperstepKickoffLatchTest {

    @Test
    void testWaitFromOne() throws Exception {
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
        assertThat(wd.getError()).isNull();
        assertThat(w.getError()).isNull();
    }

    @Test
    void testWaitAlreadyFulfilled() throws Exception {
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
        assertThat(wd.getError()).isNull();
        assertThat(w.getError()).isNull();
    }

    @Test
    void testWaitIncorrect() throws Exception {
        SuperstepKickoffLatch latch = new SuperstepKickoffLatch();
        latch.triggerNextSuperstep();
        latch.triggerNextSuperstep();

        assertThatThrownBy(() -> latch.awaitStartOfSuperstepOrTermination(2))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testWaitIncorrectAsync() throws Exception {
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
        assertThat(wd.getError()).isNull();
        assertThat(w.getError()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testWaitForTermination() throws Exception {
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
        assertThat(wd.getError()).isNull();
        assertThat(w.getError()).isNull();
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
                        this.failed =
                                new Exception(
                                        "Watched thread did not respond to interrupt within 2s");
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
