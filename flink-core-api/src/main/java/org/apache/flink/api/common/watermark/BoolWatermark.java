/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.watermark;

import org.apache.flink.annotation.Experimental;

import java.util.Objects;

/**
 * The {@link BoolWatermark} represents a watermark with a boolean value and an associated
 * identifier.
 */
@Experimental
public class BoolWatermark implements Watermark {

    private static final long serialVersionUID = 1L;

    private final boolean value;

    private final String identifier;

    public BoolWatermark(boolean value, String identifier) {
        this.value = value;
        this.identifier = identifier;
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoolWatermark that = (BoolWatermark) o;
        return value == that.value && Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, identifier);
    }

    @Override
    public String toString() {
        return "BoolWatermark{" + "value=" + value + ", identifier='" + identifier + '\'' + '}';
    }
}
