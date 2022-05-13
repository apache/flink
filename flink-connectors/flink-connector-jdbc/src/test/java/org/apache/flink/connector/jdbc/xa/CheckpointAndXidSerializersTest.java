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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** XaSerializersTest. */
public class CheckpointAndXidSerializersTest extends SerializerTestBase<CheckpointAndXid> {
    private static final CheckpointAndXid CHECKPOINT_AND_XID =
            CheckpointAndXid.createRestored(1L, 1, XidImplTest.XID);

    @Override
    protected TypeSerializer<CheckpointAndXid> createSerializer() {
        return new CheckpointAndXidSerializer();
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<CheckpointAndXid> getTypeClass() {
        return CheckpointAndXid.class;
    }

    @Override
    protected CheckpointAndXid[] getTestData() {
        return new CheckpointAndXid[] {CHECKPOINT_AND_XID};
    }
}
