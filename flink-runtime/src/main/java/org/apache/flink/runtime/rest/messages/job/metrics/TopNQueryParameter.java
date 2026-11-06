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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

/**
 * Query parameter specifying how many entries to return per dimension on the Top N metrics
 * endpoint. Defaults are applied by the handler when the parameter is absent.
 */
public class TopNQueryParameter extends MessageQueryParameter<Integer> {

    public static final String KEY = "topN";

    /** Hard upper bound to prevent pathological requests from scanning the entire metric store. */
    public static final int MAX_TOP_N = 100;

    public TopNQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public Integer convertStringToValue(String value) throws ConversionException {
        final int topN;
        try {
            topN = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ConversionException("topN must be an integer, was: " + value);
        }
        if (topN < 1) {
            throw new ConversionException("topN must be >= 1, was: " + topN);
        }
        if (topN > MAX_TOP_N) {
            throw new ConversionException("topN must be <= " + MAX_TOP_N + ", was: " + topN);
        }
        return topN;
    }

    @Override
    public String convertValueToString(Integer value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "Positive integer (1.."
                + MAX_TOP_N
                + ") controlling how many entries are returned for each Top N dimension. "
                + "Defaults to 5 when omitted.";
    }
}
