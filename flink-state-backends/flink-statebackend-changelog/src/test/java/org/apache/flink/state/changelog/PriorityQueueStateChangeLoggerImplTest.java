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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.state.changelog.StateChangeOperation.REMOVE_FIRST_ELEMENT;

/** {@link PriorityQueueStateChangeLoggerImpl} test. */
public class PriorityQueueStateChangeLoggerImplTest extends StateChangeLoggerTestBase<Void> {

    @Override
    protected StateChangeLogger<String, Void> getLogger(
            TestingStateChangelogWriter writer, InternalKeyContextImpl<String> keyContext) {
        StringSerializer valueSerializer = new StringSerializer();
        RegisteredPriorityQueueStateBackendMetaInfo<String> metaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>("test", valueSerializer);
        return new PriorityQueueStateChangeLoggerImpl<>(
                valueSerializer, keyContext, writer, metaInfo);
    }

    @Override
    protected Optional<Tuple2<Integer, StateChangeOperation>> log(
            StateChangeOperation op,
            String element,
            StateChangeLogger<String, Void> logger,
            InternalKeyContextImpl<String> keyContext)
            throws IOException {
        if (op == REMOVE_FIRST_ELEMENT) {
            keyContext.setCurrentKey(element);
            ((PriorityQueueStateChangeLogger<String>) logger).stateElementPolled();
            return Optional.of(Tuple2.of(keyContext.getCurrentKeyGroupIndex(), op));
        } else {
            return super.log(op, element, logger, keyContext);
        }
    }

    @Override
    protected Void getNamespace(String element) {
        return null;
    }
}
