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

package org.apache.flink.connector.pulsar.testutils.runtime.mock;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;

/**
 * Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test.
 */
public class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

    private NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
        super(executor);
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public void shutdown() {
        // no-op
    }

    public void reallyShutdown() {
        super.shutdown();
    }

    public static BookKeeper create(OrderedExecutor executor) {
        try {
            return new NonClosableMockBookKeeper(executor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
