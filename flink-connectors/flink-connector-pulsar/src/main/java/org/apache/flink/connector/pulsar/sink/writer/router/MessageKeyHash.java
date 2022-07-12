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
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import org.apache.pulsar.client.impl.Hash;
import org.apache.pulsar.client.impl.JavaStringHash;
import org.apache.pulsar.client.impl.Murmur3Hash32;

import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Predefined the available hash function for routing the message. */
@PublicEvolving
public enum MessageKeyHash implements DescribedEnum {

    /** Use regular <code>String.hashCode()</code>. */
    JAVA_HASH(
            "java-hash",
            text(
                    "This hash would use %s to calculate the message key string's hash code.",
                    code("String.hashCode()"))) {
        @Override
        public Hash getHash() {
            return JavaStringHash.getInstance();
        }
    },
    /**
     * Use Murmur3 hashing function. <a
     * href="https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash</a>
     */
    MURMUR3_32_HASH(
            "murmur-3-32-hash",
            text(
                    "This hash would calculate message key's hash code by using %s algorithm.",
                    link("https://en.wikipedia.org/wiki/MurmurHash", "Murmur3"))) {
        @Override
        public Hash getHash() {
            return Murmur3Hash32.getInstance();
        }
    };

    private final String name;
    private final InlineElement desc;

    MessageKeyHash(String name, InlineElement desc) {
        this.name = name;
        this.desc = desc;
    }

    @Internal
    public abstract Hash getHash();

    @Override
    public String toString() {
        return name;
    }

    @Internal
    @Override
    public InlineElement getDescription() {
        return desc;
    }
}
