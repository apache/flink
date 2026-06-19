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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** Serializer to serialize {@link FetchResultsResponseBody}. */
public class FetchResultsResponseBodySerializer extends StdSerializer<FetchResultsResponseBody> {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_RESULT_TYPE = "resultType";
    public static final String FIELD_IS_QUERY_RESULT = "isQueryResult";
    public static final String FIELD_JOB_ID = "jobID";
    public static final String FIELD_RESULT_KIND = "resultKind";
    public static final String FIELD_RESULTS = "results";
    public static final String FIELD_NEXT_RESULT_URI = "nextResultUri";

    public FetchResultsResponseBodySerializer() {
        super(FetchResultsResponseBody.class);
    }

    @Override
    public void serialize(
            FetchResultsResponseBody value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {

        gen.writeStartObject();
        provider.defaultSerializeField(FIELD_RESULT_TYPE, value.getResultType(), gen);
        if (value.getResultType() != ResultSet.ResultType.NOT_READY) {
            // PAYLOAD or EOS
            provider.defaultSerializeField(FIELD_IS_QUERY_RESULT, value.isQueryResult(), gen);
            if (value.getJobID() != null) {
                provider.defaultSerializeField(FIELD_JOB_ID, value.getJobID().toHexString(), gen);
            }
            provider.defaultSerializeField(FIELD_RESULT_KIND, value.getResultKind(), gen);
            provider.defaultSerializeField(FIELD_RESULTS, value.getResults(), gen);
        }
        if (value.getNextResultUri() != null) {
            gen.writeStringField(FIELD_NEXT_RESULT_URI, value.getNextResultUri());
        }
        gen.writeEndObject();
    }
}
