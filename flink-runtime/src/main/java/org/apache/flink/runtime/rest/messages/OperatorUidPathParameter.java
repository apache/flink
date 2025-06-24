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

package org.apache.flink.runtime.rest.messages;

/** Path parameter identifying operators. */
public class OperatorUidPathParameter extends MessagePathParameter<String> {

    /**
     * This id must be defined to identify an operator on the client side before submit jobs.
     * Otherwise, the query cannot be executed correctly. Note that we use operatorUid instead of
     * operatorID because the latter is an internal runtime concept that cannot be recognized by the
     * client.
     */
    public static final String KEY = "operatorUid";

    public OperatorUidPathParameter() {
        super(KEY);
    }

    @Override
    protected String convertFromString(String value) throws ConversionException {
        return value;
    }

    @Override
    protected String convertToString(String value) {
        return value;
    }

    @Override
    public String getDescription() {
        return "string value that identifies an operator.";
    }
}
