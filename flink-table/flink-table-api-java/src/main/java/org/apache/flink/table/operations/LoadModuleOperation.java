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

package org.apache.flink.table.operations;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operation to describe a LOAD MODULE statement. */
public class LoadModuleOperation implements Operation {
    private final String moduleName;

    private final Map<String, String> properties;

    public LoadModuleOperation(String moduleName, Map<String, String> properties) {
        this.moduleName = checkNotNull(moduleName);
        this.properties = checkNotNull(properties);
    }

    public String getModuleName() {
        return moduleName;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("moduleName", moduleName);
        params.put("properties", properties);
        return OperationUtils.formatWithChildren(
                "LOAD MODULE", params, Collections.emptyList(), Operation::asSummaryString);
    }
}
