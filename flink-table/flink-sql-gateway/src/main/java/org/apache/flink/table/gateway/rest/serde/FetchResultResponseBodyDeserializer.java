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

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultResponseBodyImpl;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.NotReadyFetchResultResponse;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

import static org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer.FIELD_IS_QUERY_RESULT;
import static org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer.FIELD_JOB_ID;
import static org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer.FIELD_NEXT_RESULT_URI;
import static org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer.FIELD_RESULTS;
import static org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer.FIELD_RESULT_KIND;
import static org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer.FIELD_RESULT_TYPE;

/** Deserializer to deserialize {@link FetchResultsResponseBody}. */
public class FetchResultResponseBodyDeserializer extends StdDeserializer<FetchResultsResponseBody> {

    private static final long serialVersionUID = 1L;

    protected FetchResultResponseBodyDeserializer() {
        super(FetchResultsResponseBody.class);
    }

    @Override
    public FetchResultsResponseBody deserialize(
            JsonParser jsonParser, DeserializationContext context) throws IOException {
        final JsonNode jsonNode = jsonParser.readValueAsTree();

        ResultSet.ResultType resultType = getResultType(jsonNode);
        if (resultType == ResultSet.ResultType.NOT_READY) {
            return new NotReadyFetchResultResponse(
                    jsonNode.required(FIELD_NEXT_RESULT_URI).asText());
        }

        boolean isQuery = jsonNode.required(FIELD_IS_QUERY_RESULT).asBoolean();
        ResultInfo result =
                context.readValue(
                        jsonNode.required(FIELD_RESULTS).traverse(jsonParser.getCodec()),
                        ResultInfo.class);
        ResultKind resultKind = ResultKind.valueOf(jsonNode.required(FIELD_RESULT_KIND).asText());
        JobID jobID =
                jsonNode.has(FIELD_JOB_ID)
                        ? JobID.fromHexString(jsonNode.get(FIELD_JOB_ID).asText())
                        : null;
        return new FetchResultResponseBodyImpl(
                resultType,
                isQuery,
                jobID,
                resultKind,
                result,
                jsonNode.has(FIELD_NEXT_RESULT_URI)
                        ? jsonNode.get(FIELD_NEXT_RESULT_URI).asText()
                        : null);
    }

    private ResultSet.ResultType getResultType(JsonNode serialized) throws IOException {
        if (!serialized.has(FIELD_RESULT_TYPE)) {
            throw new IOException(
                    String.format("Can not find the required field %s.", FIELD_RESULT_TYPE));
        }
        return ResultSet.ResultType.valueOf(serialized.get(FIELD_RESULT_TYPE).asText());
    }
}
