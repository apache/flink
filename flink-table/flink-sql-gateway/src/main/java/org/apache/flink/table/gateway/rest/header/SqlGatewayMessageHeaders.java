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

package org.apache.flink.table.gateway.rest.header;

import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This class links {@link RequestBody}s to {@link ResponseBody}s types and contains meta-data
 * required for their http headers in runtime module.
 *
 * <p>Implementations must be state-less.
 *
 * @param <R> request message type
 * @param <P> response message type
 * @param <M> message parameters type
 */
public interface SqlGatewayMessageHeaders<
                R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
        extends MessageHeaders<R, P, M> {

    @Override
    default Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Arrays.stream(SqlGatewayRestAPIVersion.values())
                .filter(SqlGatewayRestAPIVersion::isStableVersion)
                .collect(Collectors.toList());
    }
}
