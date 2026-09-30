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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.functions.XmlToVariantParser;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#TRY_PARSE_XML}. */
@Internal
public class TryParseXmlFunction extends BuiltInScalarFunction {

    private transient XmlToVariantParser parser;

    public TryParseXmlFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TRY_PARSE_XML, context);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        parser = new XmlToVariantParser();
    }

    public @Nullable Variant eval(@Nullable StringData xmlStr) {
        return eval(xmlStr, false);
    }

    public @Nullable Variant eval(@Nullable StringData xmlStr, @Nullable Boolean forceArray) {
        if (xmlStr == null || forceArray == null) {
            return null;
        }

        try {
            return parser.parse(xmlStr.toString(), forceArray);
        } catch (Throwable e) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
            return null;
        }
    }
}
