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

package org.apache.flink.table.gateway.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/** {@link RefreshHandlerSerializer} for {@link EmbeddedRefreshHandler}. */
@PublicEvolving
public class EmbeddedRefreshHandlerSerializer
        implements RefreshHandlerSerializer<EmbeddedRefreshHandler> {

    public static final EmbeddedRefreshHandlerSerializer INSTANCE =
            new EmbeddedRefreshHandlerSerializer();

    @Override
    public byte[] serialize(EmbeddedRefreshHandler refreshHandler) throws IOException {
        return InstantiationUtil.serializeObject(refreshHandler);
    }

    @Override
    public EmbeddedRefreshHandler deserialize(byte[] serializedBytes, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        return InstantiationUtil.deserializeObject(serializedBytes, cl);
    }
}
