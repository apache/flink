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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;

/** Validator for {@code StreamTableDescriptor}. */
@Internal
public class StreamTableDescriptorValidator implements DescriptorValidator {

    public static final String UPDATE_MODE = "update-mode";
    public static final String UPDATE_MODE_VALUE_APPEND = "append";
    public static final String UPDATE_MODE_VALUE_RETRACT = "retract";
    public static final String UPDATE_MODE_VALUE_UPSERT = "upsert";

    private final boolean supportsAppend;
    private final boolean supportsRetract;
    private final boolean supportsUpsert;

    public StreamTableDescriptorValidator(
            boolean supportsAppend, boolean supportsRetract, boolean supportsUpsert) {
        this.supportsAppend = supportsAppend;
        this.supportsRetract = supportsRetract;
        this.supportsUpsert = supportsUpsert;
    }

    @Override
    public void validate(DescriptorProperties properties) {
        List<String> modeList = new ArrayList<>();
        if (supportsAppend) {
            modeList.add(UPDATE_MODE_VALUE_APPEND);
        }
        if (supportsRetract) {
            modeList.add(UPDATE_MODE_VALUE_RETRACT);
        }
        if (supportsUpsert) {
            modeList.add(UPDATE_MODE_VALUE_UPSERT);
        }
        properties.validateEnumValues(UPDATE_MODE, false, modeList);
    }
}
