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

package org.apache.flink.runtime.asyncprocessing;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link ReferenceCounted}. */
class ReferenceCountedTest {
    @Test
    void testRefCountReachedZero() {
        TestReferenceCounted referenceCounted = new TestReferenceCounted();
        referenceCounted.retain();
        assertThat(referenceCounted.getReferenceCount()).isEqualTo(1);
        referenceCounted.release();
        assertThat(referenceCounted.getReferenceCount()).isEqualTo(0);
        assertThat(referenceCounted.reachedZero).isTrue();
    }

    @Test
    void testConcurrency() throws InterruptedException {
        TestReferenceCounted referenceCounted = new TestReferenceCounted();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(referenceCounted::retain);
            thread.start();
            threads.add(thread);
        }
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(referenceCounted::release);
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(referenceCounted.getReferenceCount()).isEqualTo(0);
    }

    private static class TestReferenceCounted extends ReferenceCounted<Void> {
        private boolean reachedZero = false;

        public TestReferenceCounted() {
            super(0);
        }

        @Override
        protected void referenceCountReachedZero(Void v) {
            reachedZero = true;
        }
    }
}
