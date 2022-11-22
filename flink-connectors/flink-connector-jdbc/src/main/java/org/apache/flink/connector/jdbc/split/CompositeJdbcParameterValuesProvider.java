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

package org.apache.flink.connector.jdbc.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Combine 2 {@link JdbcParameterValuesProvider} into 1. */
@Internal
public class CompositeJdbcParameterValuesProvider implements JdbcParameterValuesProvider {
    JdbcParameterValuesProvider a;
    JdbcParameterValuesProvider b;

    public CompositeJdbcParameterValuesProvider(
            JdbcParameterValuesProvider a, JdbcParameterValuesProvider b) {
        Preconditions.checkArgument(
                a.getParameterValues().length == b.getParameterValues().length,
                "Both JdbcParameterValuesProvider should have the same length.");
        this.a = a;
        this.b = b;
    }

    @Override
    public Serializable[][] getParameterValues() {
        int batchNum = this.a.getParameterValues().length;
        Serializable[][] parameters = new Serializable[batchNum][];
        for (int i = 0; i < batchNum; i++) {
            Serializable[] aSlice = a.getParameterValues()[i];
            Serializable[] bSlice = b.getParameterValues()[i];
            int totalLen = aSlice.length + bSlice.length;

            Serializable[] batchParams = new Serializable[totalLen];

            System.arraycopy(aSlice, 0, batchParams, 0, aSlice.length);
            System.arraycopy(bSlice, 0, batchParams, aSlice.length, bSlice.length);
            parameters[i] = batchParams;
        }
        return parameters;
    }
}
