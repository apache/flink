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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.api.common.state.StateDescriptor.Type.VALUE;
import static org.apache.flink.state.changelog.StateChangeOperation.MERGE_NS;

/** {@link KvStateChangeLoggerImpl} test. */
public class KvStateChangeLoggerImplTest extends StateChangeLoggerTestBase<String> {

    @Override
    protected StateChangeLogger<String, String> getLogger(
            TestingStateChangelogWriter writer, InternalKeyContextImpl<String> keyContext) {
        StringSerializer keySerializer = new StringSerializer();
        StringSerializer nsSerializer = new StringSerializer();
        StringSerializer valueSerializer = new StringSerializer();
        RegisteredKeyValueStateBackendMetaInfo<String, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        VALUE, "test", nsSerializer, valueSerializer);
        return new KvStateChangeLoggerImpl<>(
                keySerializer,
                nsSerializer,
                valueSerializer,
                keyContext,
                writer,
                metaInfo,
                StateTtlConfig.DISABLED,
                "default");
    }

    @Override
    protected String getNamespace(String element) {
        return element;
    }

    @Override
    protected Optional<Tuple2<Integer, StateChangeOperation>> log(
            StateChangeOperation op,
            String element,
            StateChangeLogger<String, String> logger,
            InternalKeyContextImpl<String> keyContext)
            throws IOException {
        if (op == MERGE_NS) {
            keyContext.setCurrentKey(element);
            ((KvStateChangeLogger<String, String>) logger)
                    .namespacesMerged(element, Collections.emptyList());
            return Optional.of(Tuple2.of(keyContext.getCurrentKeyGroupIndex(), op));
        } else {
            return super.log(op, element, logger, keyContext);
        }
    }
}
