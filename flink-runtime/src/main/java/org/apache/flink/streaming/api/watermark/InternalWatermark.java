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

package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** The {@link Watermark} that contains extra information to be used internally. */
@Internal
public final class InternalWatermark extends Watermark {
    private final int subpartitionIndex;

    public InternalWatermark(long timestamp, int subpartitionIndex) {
        super(timestamp);
        this.subpartitionIndex = subpartitionIndex;
    }

    public int getSubpartitionIndex() {
        return subpartitionIndex;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && ((InternalWatermark) o).subpartitionIndex == subpartitionIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subpartitionIndex);
    }

    @Override
    public String toString() {
        return "InternalWatermark @ " + timestamp + " " + subpartitionIndex;
    }
}
