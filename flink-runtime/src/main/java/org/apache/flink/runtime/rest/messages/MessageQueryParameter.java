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

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents query parameters of a request. For example, the URL "/jobs?state=running"
 * has a "state" query parameter, with "running" being its value string representation.
 *
 * <p>Query parameters may both occur multiple times or be of the form "key=value1,value2,value3".
 * If a query parameter is specified multiple times the individual values are concatenated with
 * {@code ,} and passed as a single value to {@link #convertToString(List)}.
 */
public abstract class MessageQueryParameter<X> extends MessageParameter<List<X>> {
    protected MessageQueryParameter(String key, MessageParameterRequisiteness requisiteness) {
        super(key, requisiteness);
    }

    @Override
    public List<X> convertFromString(String values) throws ConversionException {
        String[] splitValues = values.split(",");
        List<X> list = new ArrayList<>();
        for (String value : splitValues) {
            list.add(convertStringToValue(value));
        }
        return list;
    }

    /**
     * Converts the given string to a valid value of this parameter.
     *
     * @param value string representation of parameter value
     * @return parameter value
     */
    public abstract X convertStringToValue(String value) throws ConversionException;

    @Override
    public String convertToString(List<X> values) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (X value : values) {
            if (first) {
                sb.append(convertValueToString(value));
                first = false;
            } else {
                sb.append(",");
                sb.append(convertValueToString(value));
            }
        }
        return sb.toString();
    }

    /**
     * Converts the given value to its string representation.
     *
     * @param value parameter value
     * @return string representation of typed value
     */
    public abstract String convertValueToString(X value);
}
