/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ResourceGuard}. */
class ResourceGuardTest {

    @Test
    void testClose() {
        ResourceGuard resourceGuard = new ResourceGuard();
        assertThat(resourceGuard.isClosed()).isFalse();
        resourceGuard.close();
        assertThat(resourceGuard.isClosed()).isTrue();
        assertThatThrownBy(resourceGuard::acquireResource).isInstanceOf(IOException.class);
    }

    @Test
    void testAcquireReleaseClose() throws IOException {
        ResourceGuard resourceGuard = new ResourceGuard();
        ResourceGuard.Lease lease = resourceGuard.acquireResource();
        assertThat(resourceGuard.getLeaseCount()).isOne();
        lease.close();
        assertThat(resourceGuard.getLeaseCount()).isZero();
        resourceGuard.close();
        assertThat(resourceGuard.isClosed()).isTrue();
    }

    @Test
    void testCloseBlockIfAcquired() throws Exception {
        ResourceGuard resourceGuard = new ResourceGuard();
        ResourceGuard.Lease lease = resourceGuard.acquireResource();
        AtomicBoolean checker = new AtomicBoolean(true);

        Thread closerThread =
                new Thread(
                        () -> {
                            // this line should block until all acquires are matched by releases.
                            resourceGuard.close();
                            checker.set(false);
                        });

        closerThread.start();

        // we wait until the close()-call in the other thread happened.
        while (!resourceGuard.isClosed()) {
            Thread.yield();
        }

        // validate that the close()-call is still blocked.
        assertThat(checker.get()).isTrue();

        // validate that the closed-status is already effective.
        assertThatThrownBy(resourceGuard::acquireResource).isInstanceOf(IOException.class);

        // this matches the first acquire and will unblock the close()-call in the other thread.
        lease.close();
        closerThread.join(60_000);
        assertThat(checker.get()).isFalse();
    }

    @Test
    void testInterruptHandledCorrectly() throws Exception {
        ResourceGuard resourceGuard = new ResourceGuard();
        ResourceGuard.Lease lease = resourceGuard.acquireResource();
        AtomicBoolean checker = new AtomicBoolean(true);

        Thread closerThread =
                new Thread(
                        () -> {
                            // this line should block until all acquires are matched by releases.
                            resourceGuard.close();
                            checker.set(false);
                        });

        closerThread.start();

        // we wait until the close()-call in the other thread happened.
        while (!resourceGuard.isClosed()) {
            Thread.yield();
        }

        // attempt to unblock the resource guard via interrupt.
        closerThread.interrupt();

        // wait some time.
        closerThread.join(100);

        // check that unblock through interrupting failed.
        assertThat(checker.get()).isTrue();

        // proper unblocking by closing the lease.
        lease.close();
        closerThread.join(60_000);
        assertThat(checker.get()).isFalse();
    }

    @Test
    void testLeaseCloseIsIdempotent() throws Exception {
        ResourceGuard resourceGuard = new ResourceGuard();
        ResourceGuard.Lease lease1 = resourceGuard.acquireResource();
        ResourceGuard.Lease lease2 = resourceGuard.acquireResource();
        assertThat(resourceGuard.getLeaseCount()).isEqualTo(2);
        lease1.close();
        assertThat(resourceGuard.getLeaseCount()).isOne();
        lease1.close();
        assertThat(resourceGuard.getLeaseCount()).isOne();
        lease2.close();
        assertThat(resourceGuard.getLeaseCount()).isZero();
        ResourceGuard.Lease lease3 = resourceGuard.acquireResource();
        assertThat(resourceGuard.getLeaseCount()).isOne();
        lease2.close();
        assertThat(resourceGuard.getLeaseCount()).isOne();
        lease3.close();
        assertThat(resourceGuard.getLeaseCount()).isZero();
        resourceGuard.close();
    }
}
