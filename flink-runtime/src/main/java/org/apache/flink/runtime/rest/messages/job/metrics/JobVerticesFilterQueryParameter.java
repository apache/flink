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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

/** {@link MessageQueryParameter} for selecting job vertices when aggregating subtask metrics. */
public class JobVerticesFilterQueryParameter extends MessageQueryParameter<JobVertexID> {

    public JobVerticesFilterQueryParameter() {
        super("vertices", MessageParameterRequisiteness.MANDATORY);
    }

    @Override
    public JobVertexID convertStringToValue(String value) throws ConversionException {
        try {
            return JobVertexID.fromHexString(value);
        } catch (IllegalArgumentException iae) {
            throw new ConversionException("Not a valid job vertex ID: " + value, iae);
        }
    }

    @Override
    public String convertValueToString(JobVertexID value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "Comma-separated list of 32-character hexadecimal strings to select specific job vertices.";
    }
}
