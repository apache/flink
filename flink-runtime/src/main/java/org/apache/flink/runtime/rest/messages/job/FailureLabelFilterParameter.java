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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Specifies a collections of failure labels, filtering the exceptions returned for
 * JobExceptionsHandler.
 *
 * @see org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler
 */
public class FailureLabelFilterParameter
        extends MessageQueryParameter<FailureLabelFilterParameter.FailureLabel> {

    public static final String KEY = "failureLabelFilter";

    /** Represents a failure label consisting of a KV pair of strings. */
    public static class FailureLabel {

        private final String key;
        private final String value;

        public FailureLabel(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }

    public FailureLabelFilterParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public List<FailureLabel> convertFromString(String values) throws ConversionException {
        String[] splitValues = values.split(",");
        Set<FailureLabel> result = new HashSet<>();
        for (String value : splitValues) {
            result.add(convertStringToValue(value));
        }
        return new ArrayList<>(result);
    }

    @Override
    public FailureLabel convertStringToValue(String value) throws ConversionException {
        String[] tokens = value.split(":");
        if (tokens.length != 2) {
            throw new ConversionException(
                    String.format("%s may be a `key:value` entry only (%s)", KEY, value));
        }
        return new FailureLabel(tokens[0], tokens[1]);
    }

    @Override
    public String convertValueToString(FailureLabel value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "Collection of string values working as a filter in the form of `key:value` pairs "
                + "allowing only exceptions with ALL of the specified failure labels to be returned.";
    }
}
