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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.Module;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.table.utils.EncodingUtils.escapeSingleQuotes;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operation to describe a LOAD MODULE statement. */
@Internal
public class LoadModuleOperation implements Operation, ExecutableOperation {

    private final String moduleName;
    private final Map<String, String> options;

    public LoadModuleOperation(String moduleName, Map<String, String> options) {
        this.moduleName = checkNotNull(moduleName);
        this.options = checkNotNull(options);
    }

    public String getModuleName() {
        return moduleName;
    }

    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public String asSummaryString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("LOAD MODULE ");
        sb.append(escapeIdentifier(moduleName));
        if (!options.isEmpty()) {
            sb.append(" WITH (");

            sb.append(
                    options.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "'%s' = '%s'",
                                                    escapeSingleQuotes(entry.getKey()),
                                                    escapeSingleQuotes(entry.getValue())))
                            .collect(Collectors.joining(", ")));

            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            final Module module =
                    FactoryUtil.createModule(
                            getModuleName(),
                            getOptions(),
                            ctx.getTableConfig(),
                            ctx.getResourceManager().getUserClassLoader());
            ctx.getModuleManager().loadModule(getModuleName(), module);
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (ValidationException e) {
            throw new ValidationException(
                    String.format("Could not execute %s. %s", asSummaryString(), e.getMessage()),
                    e);
        } catch (Exception e) {
            throw new TableException(
                    String.format("Could not execute %s. %s", asSummaryString(), e.getMessage()),
                    e);
        }
    }
}
