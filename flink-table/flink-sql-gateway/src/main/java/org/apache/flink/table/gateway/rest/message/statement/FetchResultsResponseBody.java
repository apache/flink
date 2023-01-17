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

package org.apache.flink.table.gateway.rest.message.statement;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.rest.serde.FetchResultResponseBodyDeserializer;
import org.apache.flink.table.gateway.rest.serde.FetchResultsResponseBodySerializer;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

/** {@link ResponseBody} for executing a statement. */
@JsonSerialize(using = FetchResultsResponseBodySerializer.class)
@JsonDeserialize(using = FetchResultResponseBodyDeserializer.class)
public interface FetchResultsResponseBody extends ResponseBody {

    ResultInfo getResults();

    ResultSet.ResultType getResultType();

    @Nullable
    String getNextResultUri();

    boolean isQueryResult();

    @Nullable
    JobID getJobID();

    ResultKind getResultKind();
}
