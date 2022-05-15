/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.mongodb.util.SerializationUtils;

import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** MongoDb implementation {@link AsyncSinkWriterStateSerializer}. */
@Internal
public class MongoDbStateSerializer
        extends AsyncSinkWriterStateSerializer<MongoDbWriteModel<? extends WriteModel<Document>>> {

    @Override
    protected void serializeRequestToStream(
            MongoDbWriteModel<? extends WriteModel<Document>> request, DataOutputStream out)
            throws IOException {
        SerializationUtils.serializeMongoDbWriteModel(request, out);
    }

    @Override
    protected MongoDbWriteModel<? extends WriteModel<Document>> deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        return SerializationUtils.deserializeMongoDbWriteModel(in);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
