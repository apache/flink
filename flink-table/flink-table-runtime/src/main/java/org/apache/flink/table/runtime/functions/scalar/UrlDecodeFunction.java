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
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Implementation of {@link BuiltInFunctionDefinitions#URL_DECODE}. */
@Internal
public class UrlDecodeFunction extends BuiltInScalarFunction {

    public UrlDecodeFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.URL_DECODE, context);
    }

    public @Nullable StringData eval(StringData value) {
        if (value == null) {
            return null;
        }
        final Charset charset = StandardCharsets.UTF_8;
        try {
            return StringData.fromString(URLDecoder.decode(value.toString(), charset.name()));
        } catch (UnsupportedEncodingException | RuntimeException e) {
            return null;
        }
    }
}
