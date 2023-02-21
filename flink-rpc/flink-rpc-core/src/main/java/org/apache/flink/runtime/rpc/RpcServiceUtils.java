/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc;

import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/** These RPC utilities contain helper methods around RPC use. */
public class RpcServiceUtils {
    private static final AtomicLong nextNameOffset = new AtomicLong(0L);

    /**
     * Creates a random name of the form prefix_X, where X is an increasing number.
     *
     * @param prefix Prefix string to prepend to the monotonically increasing name offset number
     * @return A random name of the form prefix_X where X is an increasing number
     */
    public static String createRandomName(String prefix) {
        Preconditions.checkNotNull(prefix, "Prefix must not be null.");

        long nameOffset;

        // obtain the next name offset by incrementing it atomically
        do {
            nameOffset = nextNameOffset.get();
        } while (!nextNameOffset.compareAndSet(nameOffset, nameOffset + 1L));

        return prefix + '_' + nameOffset;
    }

    /**
     * Creates a wildcard name symmetric to {@link #createRandomName(String)}.
     *
     * @param prefix prefix of the wildcard name
     * @return wildcard name starting with the prefix
     */
    public static String createWildcardName(String prefix) {
        return prefix + "_*";
    }
}
