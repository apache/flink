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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Extract key and topic from a value.
 */
public interface TopicKeyExtractor<T> extends Serializable {

    TopicKeyExtractor DUMMY_FOR_ROW = new TopicKeyExtractor<Row>() {
        @Override
        public byte[] serializeKey(Row element) {
            return null;
        }

        @Override
        public String getTopic(Row element) {
            return null;
        }
    };

    byte[] serializeKey(T element);

    String getTopic(T element);
}
