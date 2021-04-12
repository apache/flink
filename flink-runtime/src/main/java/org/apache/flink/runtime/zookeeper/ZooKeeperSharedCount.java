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

package org.apache.flink.runtime.zookeeper;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.shared.SharedCount;

import java.io.IOException;

/**
 * Wrapper class for a {@link SharedCount} so that we don't expose a curator dependency in our
 * internal APIs. Such an exposure is problematic due to the relocation of curator.
 */
public class ZooKeeperSharedCount {

    private final SharedCount sharedCount;

    public ZooKeeperSharedCount(SharedCount sharedCount) {
        this.sharedCount = Preconditions.checkNotNull(sharedCount);
    }

    public void start() throws Exception {
        sharedCount.start();
    }

    public void close() throws IOException {
        sharedCount.close();
    }

    public ZooKeeperVersionedValue<Integer> getVersionedValue() {
        return new ZooKeeperVersionedValue<>(sharedCount.getVersionedValue());
    }

    public boolean trySetCount(ZooKeeperVersionedValue<Integer> previous, int newCount)
            throws Exception {
        return sharedCount.trySetCount(previous.getVersionedValue(), newCount);
    }
}
