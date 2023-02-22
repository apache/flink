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

package org.apache.flink.table.gateway.rest.header.operation;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.table.gateway.rest.message.operation.OperationHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;

/** Message headers for canceling operation. */
public class CancelOperationHeaders extends AbstractOperationHeaders {

    private static final CancelOperationHeaders INSTANCE = new CancelOperationHeaders();

    private static final String URL =
            "/sessions/:"
                    + SessionHandleIdPathParameter.KEY
                    + "/operations/:"
                    + OperationHandleIdPathParameter.KEY
                    + "/cancel";

    @Override
    public String getDescription() {
        return "Cancel the operation.";
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static CancelOperationHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String operationId() {
        return "cancelOperation";
    }
}
