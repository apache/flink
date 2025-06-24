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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import java.util.Objects;
import java.util.Random;

/** {@link NettyConnectionId} indicates the unique id of netty connection. */
public class NettyConnectionId {

    private static final Random RANDOM_SEED = new Random();

    private final long lowerPart;

    private final long upperPart;

    private NettyConnectionId(long lowerPart, long upperPart) {
        this.lowerPart = lowerPart;
        this.upperPart = upperPart;
    }

    public static NettyConnectionId newId() {
        return new NettyConnectionId(RANDOM_SEED.nextLong(), RANDOM_SEED.nextLong());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NettyConnectionId that = (NettyConnectionId) o;
        return lowerPart == that.lowerPart && upperPart == that.upperPart;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowerPart, upperPart);
    }
}
