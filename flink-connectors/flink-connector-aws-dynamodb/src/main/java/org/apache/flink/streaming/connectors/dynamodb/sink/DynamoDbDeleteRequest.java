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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Represents a DynamoDb Delete Request. */
@PublicEvolving
public class DynamoDbDeleteRequest implements Serializable {

    private final Map<String, DynamoDbAttributeValue> key;

    private DynamoDbDeleteRequest(Map<String, DynamoDbAttributeValue> key) {
        this.key = key;
    }

    public Map<String, DynamoDbAttributeValue> key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbDeleteRequest that = (DynamoDbDeleteRequest) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder. */
    public static class Builder {
        private Map<String, DynamoDbAttributeValue> key;

        public Builder key(Map<String, DynamoDbAttributeValue> key) {
            this.key = key;
            return this;
        }

        public DynamoDbDeleteRequest build() {
            checkNotNull(key);
            return new DynamoDbDeleteRequest(key);
        }
    }
}
