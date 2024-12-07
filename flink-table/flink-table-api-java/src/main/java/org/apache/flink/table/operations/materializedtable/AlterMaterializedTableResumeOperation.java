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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Map;

/** Operation to describe a ALTER MATERIALIZED TABLE ... RESUME statement. */
@Internal
public class AlterMaterializedTableResumeOperation extends AlterMaterializedTableOperation {

    private final Map<String, String> dynamicOptions;

    public AlterMaterializedTableResumeOperation(
            ObjectIdentifier tableIdentifier, Map<String, String> options) {
        super(tableIdentifier);
        this.dynamicOptions = options;
    }

    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        throw new UnsupportedOperationException(
                "AlterMaterializedTableResumeOperation doesn't support ExecutableOperation yet.");
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder =
                new StringBuilder(
                        String.format(
                                "ALTER MATERIALIZED TABLE %s RESUME",
                                tableIdentifier.asSummaryString()));
        if (!dynamicOptions.isEmpty()) {
            builder.append(
                    String.format(" WITH (%s)", OperationUtils.formatProperties(dynamicOptions)));
        }
        return builder.toString();
    }
}
