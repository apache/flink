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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import java.util.Arrays;
import java.util.Collections;

/** Test suite for {@link TtlListState} with elements of serialized by kryo. */
public class TtlListStateWithKryoTestContext
        extends TtlListStateTestContextBase<TtlListStateWithKryoTestContext.NotPojoElement> {
    TtlListStateWithKryoTestContext() {
        super(new KryoSerializer<>(NotPojoElement.class, getForceKryoSerializerConfig()));
    }

    private static SerializerConfig getForceKryoSerializerConfig() {
        Configuration config = new Configuration();
        config.set(PipelineOptions.FORCE_KRYO, true);
        return new SerializerConfigImpl(config);
    }

    @Override
    NotPojoElement generateRandomElement(int i) {
        return new NotPojoElement(RANDOM.nextInt(100));
    }

    @Override
    void initTestValues() {
        emptyValue = Collections.emptyList();

        updateEmpty =
                Arrays.asList(new NotPojoElement(5), new NotPojoElement(7), new NotPojoElement(10));
        updateUnexpired =
                Arrays.asList(new NotPojoElement(8), new NotPojoElement(9), new NotPojoElement(11));
        updateExpired = Arrays.asList(new NotPojoElement(1), new NotPojoElement(4));

        getUpdateEmpty = updateEmpty;
        getUnexpired = updateUnexpired;
        getUpdateExpired = updateExpired;
    }

    public static class NotPojoElement {
        public int value;

        public NotPojoElement(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "NotPojoElement{" + "value=" + value + '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            NotPojoElement that = (NotPojoElement) obj;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(value);
        }
    }
}
