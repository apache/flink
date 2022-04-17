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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.streaming.connectors.dynamodb.util.DynamoDbSerializationUtil;

import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** DynamoDb implementation {@link AsyncSinkWriterStateSerializer}. */
@Internal
public class DynamoDbWriterStateSerializer
        extends AsyncSinkWriterStateSerializer<DynamoDbWriteRequest> {

    /**
     * Serializes {@link DynamoDbWriteRequest} in form of
     * [TABLE_NAME,WRITE_REQUEST_TYPE(PUT/DELETE),WRITE_REQUEST].
     */
    @Override
    protected void serializeRequestToStream(DynamoDbWriteRequest request, DataOutputStream out)
            throws IOException {
        out.writeUTF(request.getTableName());
        DynamoDbSerializationUtil.serializeWriteRequest(request.getWriteRequest(), out);
    }

    @Override
    protected DynamoDbWriteRequest deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        String tableName = in.readUTF();
        WriteRequest writeRequest = DynamoDbSerializationUtil.deserializeWriteRequest(in);
        return new DynamoDbWriteRequest(tableName, writeRequest);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
