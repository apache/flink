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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;

import java.util.Collections;
import java.util.List;

/** Operation to describe a USE MODULES statement. */
@Internal
public class UseModulesOperation implements UseOperation {
    private final List<String> moduleNames;

    public UseModulesOperation(List<String> moduleNames) {
        this.moduleNames = Collections.unmodifiableList(moduleNames);
    }

    public List<String> getModuleNames() {
        return moduleNames;
    }

    @Override
    public String asSummaryString() {
        return String.format("USE MODULES: %s", moduleNames);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            ctx.getModuleManager().useModules(moduleNames.toArray(new String[0]));
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw new ValidationException(
                    String.format("Could not execute %s. %s", asSummaryString(), e.getMessage()),
                    e);
        }
    }
}
