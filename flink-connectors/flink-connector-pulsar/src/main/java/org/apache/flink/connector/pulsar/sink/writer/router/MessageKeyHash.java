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

package org.apache.flink.connector.pulsar.sink.writer.router;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.impl.Hash;
import org.apache.pulsar.client.impl.JavaStringHash;
import org.apache.pulsar.client.impl.Murmur3Hash32;

/** Predefined the available hash function for routing the message. */
@PublicEvolving
public enum MessageKeyHash {

    /** Use regular <code>String.hashCode()</code>. */
    JAVA_HASH {
        @Override
        public Hash getHash() {
            return JavaStringHash.getInstance();
        }
    },
    /**
     * Use Murmur3 hashing function. <a
     * href="https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash</a>
     */
    MURMUR3_32_HASH {
        @Override
        public Hash getHash() {
            return Murmur3Hash32.getInstance();
        }
    };

    @Internal
    public abstract Hash getHash();
}
