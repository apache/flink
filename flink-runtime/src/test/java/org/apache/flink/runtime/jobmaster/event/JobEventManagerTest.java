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

package org.apache.flink.runtime.jobmaster.event;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JobEventManager}. */
class JobEventManagerTest {

    @Test
    void testStartTwice() throws Exception {
        TestingJobEventStore.init();
        JobEventManager jobEventManager = new JobEventManager(new TestingJobEventStore());

        jobEventManager.start();
        assertThat(jobEventManager.isRunning()).isTrue();
        assertThat(TestingJobEventStore.startTimes).isEqualTo(1);
        jobEventManager.start();
        assertThat(TestingJobEventStore.startTimes).isEqualTo(1);
    }

    @Test
    void testStop() throws Exception {
        TestingJobEventStore.init();
        JobEventManager jobEventManager = new JobEventManager(new TestingJobEventStore());

        jobEventManager.start();
        assertThat(jobEventManager.isRunning()).isTrue();
        jobEventManager.stop(true);
        assertThat(jobEventManager.isRunning()).isFalse();
    }

    @Test
    void testRestart() throws Exception {
        TestingJobEventStore.init();
        JobEventManager jobEventManager = new JobEventManager(new TestingJobEventStore());

        jobEventManager.start();
        jobEventManager.stop(true);
        assertThat(jobEventManager.isRunning()).isFalse();
        jobEventManager.start();
        assertThat(jobEventManager.isRunning()).isTrue();
    }

    /** Test replay event and write event before call start method. */
    @Test
    void testInvalidInvoke() {
        TestingJobEventStore.init();
        JobEventManager jobEventManager = new JobEventManager(new TestingJobEventStore());
        assertThat(jobEventManager.isRunning()).isFalse();

        // test write event before start.
        assertThatThrownBy(() -> jobEventManager.writeEvent(new TestingJobEvent(0), false))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(
                        () -> {

                            // test replay event before start.
                            jobEventManager.replay(
                                    new JobEventReplayHandler() {
                                        @Override
                                        public void startReplay() {}

                                        @Override
                                        public void replayOneEvent(JobEvent event) {}

                                        @Override
                                        public void finalizeReplay() {}
                                    });
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    public static class TestingJobEventStore implements JobEventStore {

        public static int startTimes = 0;

        @Override
        public void start() {
            startTimes++;
        }

        @Override
        public void stop(boolean clear) {}

        @Override
        public void writeEvent(JobEvent jobEvent, boolean cutBlock) {}

        @Override
        public JobEvent readEvent() {
            return null;
        }

        @Override
        public boolean isEmpty() throws Exception {
            return false;
        }

        public static void init() {
            startTimes = 0;
        }
    }
}
