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

package org.apache.flink.runtime.rest;

import java.util.Objects;

/** Represents an HTTP header with a name and a value. */
public class HttpHeader {

    /** The name of the HTTP header. */
    private final String name;

    /** The value of the HTTP header. */
    private final String value;

    /**
     * Constructs an {@code HttpHeader} object with the specified name and value.
     *
     * @param name the name of the HTTP header
     * @param value the value of the HTTP header
     */
    public HttpHeader(String name, String value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns the name of this HTTP header.
     *
     * @return the name of this HTTP header
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the value of this HTTP header.
     *
     * @return the value of this HTTP header
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "HttpHeader{" + "name='" + name + '\'' + ", value='" + value + '\'' + '}';
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        HttpHeader that = (HttpHeader) other;
        return Objects.equals(getName(), that.getName())
                && Objects.equals(getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getValue());
    }
}
