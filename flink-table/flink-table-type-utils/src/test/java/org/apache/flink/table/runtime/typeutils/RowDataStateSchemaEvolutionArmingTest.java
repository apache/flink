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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.StateSchemaEvolvingSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer.RowDataSerializerSnapshot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests which state shapes the arming {@link SerializerFactory} decorator reaches. It arms exactly
 * one structural level, so a {@link RowDataSerializer} sitting below any composite serializer stays
 * unarmed and its state fails closed at restore.
 *
 * <p>Every assertion is made on the serializer object actually reached through the state
 * descriptor. {@link RowDataSerializerSchemaEvolutionTest} covers what an armed serializer then
 * admits.
 */
class RowDataStateSchemaEvolutionArmingTest {

    /**
     * Mirrors {@code ExecutionConfigOptions.TABLE_EXEC_STATE_SCHEMA_EVOLUTION_ENABLED}, which lives
     * in a module this one cannot depend on.
     */
    private static final ConfigOption<Boolean> STATE_SCHEMA_EVOLUTION_ENABLED =
            ConfigOptions.key("table.exec.state.schema-evolution.enabled")
                    .booleanType()
                    .defaultValue(false);

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"a", "b"});

    @Test
    @SuppressWarnings("unchecked")
    void intervalJoinShapedMapStateIsNotArmed() {
        // MapState<Long, List<Tuple2<RowData, Boolean>>>, the shape the interval join registers.
        ListTypeInfo<Tuple2<RowData, Boolean>> valueTypeInfo =
                new ListTypeInfo<>(
                        new TupleTypeInfo<Tuple2<RowData, Boolean>>(
                                InternalTypeInfo.of(ROW_TYPE), Types.BOOLEAN));
        MapStateDescriptor<Long, List<Tuple2<RowData, Boolean>>> descriptor =
                new MapStateDescriptor<>("cache", Types.LONG, valueTypeInfo);

        descriptor.initializeSerializerUnlessSet(armingFactory(true));

        ListSerializer<Tuple2<RowData, Boolean>> listSerializer =
                (ListSerializer<Tuple2<RowData, Boolean>>) descriptor.getValueSerializer();
        TupleSerializer<Tuple2<RowData, Boolean>> tupleSerializer =
                (TupleSerializer<Tuple2<RowData, Boolean>>) listSerializer.getElementSerializer();
        TypeSerializer<?> nestedField = tupleSerializer.getFieldSerializers()[0];
        RowDataSerializer nested = (RowDataSerializer) nestedField;

        // The job opted in, so the nested serializer carries the opt-in bit; what it must not carry
        // is the state bit, because nothing will call migrate on a serializer two levels down.
        assertThat(nested.isSchemaEvolutionAllowed()).isTrue();
        assertThat(nested.isStateSchemaEvolutionEnabled()).isFalse();

        RowType priorRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"a"});
        RowDataSerializerSnapshot nestedSnapshot =
                (RowDataSerializerSnapshot) nested.snapshotConfiguration();
        RowDataSerializerSnapshot priorSnapshot =
                (RowDataSerializerSnapshot)
                        InternalSerializers.create(priorRowType).snapshotConfiguration();

        assertThat(nestedSnapshot.resolveSchemaCompatibility(priorSnapshot).isIncompatible())
                .isTrue();
    }

    @Test
    void valueStateIsArmed() {
        ValueStateDescriptor<RowData> descriptor =
                new ValueStateDescriptor<>("value", InternalTypeInfo.of(ROW_TYPE));

        descriptor.initializeSerializerUnlessSet(armingFactory(true));

        assertThat(((RowDataSerializer) descriptor.getSerializer()).isStateSchemaEvolutionEnabled())
                .isTrue();
    }

    @Test
    void valueStateIsNotArmedWhenTheOptionIsOff() {
        ValueStateDescriptor<RowData> descriptor =
                new ValueStateDescriptor<>("value", InternalTypeInfo.of(ROW_TYPE));

        descriptor.initializeSerializerUnlessSet(armingFactory(false));

        assertThat(((RowDataSerializer) descriptor.getSerializer()).isStateSchemaEvolutionEnabled())
                .isFalse();
    }

    @Test
    void listStateElementIsNotArmed() {
        ListStateDescriptor<RowData> descriptor =
                new ListStateDescriptor<>("list", InternalTypeInfo.of(ROW_TYPE));

        descriptor.initializeSerializerUnlessSet(armingFactory(true));

        assertThat(
                        ((RowDataSerializer) descriptor.getElementSerializer())
                                .isStateSchemaEvolutionEnabled())
                .isFalse();
    }

    private static SerializerFactory armingFactory(boolean schemaEvolutionEnabled) {
        Configuration configuration = new Configuration();
        configuration.set(STATE_SCHEMA_EVOLUTION_ENABLED, schemaEvolutionEnabled);
        SerializerConfig config = new SerializerConfigImpl(configuration);
        return StateSchemaEvolvingSerializer.arming(
                new SerializerFactory() {
                    @Override
                    public <T> TypeSerializer<T> createSerializer(
                            TypeInformation<T> typeInformation) {
                        return typeInformation.createSerializer(config);
                    }
                });
    }
}
