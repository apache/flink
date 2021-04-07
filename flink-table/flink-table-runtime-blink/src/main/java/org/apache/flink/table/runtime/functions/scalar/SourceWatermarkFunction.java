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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#SOURCE_WATERMARK}. */
@Internal
public class SourceWatermarkFunction extends BuiltInScalarFunction {

    public static final String ERROR_MESSAGE =
            String.format(
                    "SOURCE_WATERMARK() is a declarative marker function and doesn't have concrete "
                            + "runtime implementation. It can only be used as a single expression in a "
                            + "WATERMARK FOR clause in the CREATE TABLE DDL. The declaration will be "
                            + "pushed down into a table source that implements the '%s' interface. "
                            + "The source will emit system-defined watermarks afterwards. Please "
                            + "check the documentation whether the connector supports source watermarks.",
                    SupportsSourceWatermark.class.getName());

    private static final long serialVersionUID = 1L;

    public SourceWatermarkFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.SOURCE_WATERMARK, context);
    }

    public @Nullable TimestampData eval() {
        throw new TableException(ERROR_MESSAGE);
    }
}
