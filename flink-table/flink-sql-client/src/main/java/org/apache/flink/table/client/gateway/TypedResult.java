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

package org.apache.flink.table.client.gateway;

import java.util.Objects;

/**
 * Result with an attached type (actual payload, EOS, etc.).
 *
 * @param <P> type of payload
 */
public class TypedResult<P> {

    private ResultType type;

    private P payload;

    private TypedResult(ResultType type, P payload) {
        this.type = type;
        this.payload = payload;
    }

    public void setType(ResultType type) {
        this.type = type;
    }

    public void setPayload(P payload) {
        this.payload = payload;
    }

    public ResultType getType() {
        return type;
    }

    public P getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "TypedResult<" + type + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypedResult<?> that = (TypedResult<?>) o;
        return type == that.type && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, payload);
    }

    // --------------------------------------------------------------------------------------------

    public static <T> TypedResult<T> empty() {
        return new TypedResult<>(ResultType.EMPTY, null);
    }

    public static <T> TypedResult<T> payload(T payload) {
        return new TypedResult<>(ResultType.PAYLOAD, payload);
    }

    public static <T> TypedResult<T> endOfStream() {
        return new TypedResult<>(ResultType.EOS, null);
    }

    // --------------------------------------------------------------------------------------------

    /** Result types. */
    public enum ResultType {
        PAYLOAD,
        EMPTY,
        EOS
    }
}
