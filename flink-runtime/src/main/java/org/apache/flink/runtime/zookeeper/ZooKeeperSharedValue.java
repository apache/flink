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

import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.shared.SharedValue;

import java.io.IOException;

/**
 * Wrapper class for a {@link SharedValue} so that we don't expose a curator dependency in our
 * internal APIs. Such an exposure is problematic due to the relocation of curator.
 */
public class ZooKeeperSharedValue {

    private final SharedValue sharedValue;

    public ZooKeeperSharedValue(SharedValue sharedValue) {
        this.sharedValue = Preconditions.checkNotNull(sharedValue);
    }

    public void start() throws Exception {
        sharedValue.start();
    }

    public void close() throws IOException {
        sharedValue.close();
    }

    public void setValue(byte[] newValue) throws Exception {
        sharedValue.setValue(newValue);
    }

    public byte[] getValue() {
        return sharedValue.getValue();
    }
}
