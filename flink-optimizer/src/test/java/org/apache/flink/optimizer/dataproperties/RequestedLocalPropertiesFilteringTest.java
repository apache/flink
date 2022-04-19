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

package org.apache.flink.optimizer.dataproperties;

import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RequestedLocalPropertiesFilteringTest {

    private TupleTypeInfo<
                    Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
            tupleInfo =
                    new TupleTypeInfo<
                            Tuple8<
                                    Integer,
                                    Integer,
                                    Integer,
                                    Integer,
                                    Integer,
                                    Integer,
                                    Integer,
                                    Integer>>(
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO,
                            BasicTypeInfo.INT_TYPE_INFO);

    @Test
    void testNullProps() {

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setGroupedFields(new FieldSet(0, 2, 3));

        assertThatThrownBy(() -> rlProp.filterBySemanticProperties(null, 0))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testAllErased() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setGroupedFields(new FieldSet(0, 2, 3));

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testGroupingPreserved1() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"0;2;3"}, null, null, tupleInfo, tupleInfo);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setGroupedFields(new FieldSet(0, 2, 3));

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getGroupedFields()).isNotNull();
        assertThat(filtered.getGroupedFields()).hasSize(3);
        assertThat(filtered.getGroupedFields()).contains(0);
        assertThat(filtered.getGroupedFields()).contains(2);
        assertThat(filtered.getGroupedFields()).contains(3);
        assertThat(filtered.getOrdering()).isNull();
    }

    @Test
    void testGroupingPreserved2() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"3->0;5->2;1->3"}, null, null, tupleInfo, tupleInfo);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setGroupedFields(new FieldSet(0, 2, 3));

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getGroupedFields()).isNotNull();
        assertThat(filtered.getGroupedFields()).hasSize(3);
        assertThat(filtered.getGroupedFields()).contains(3);
        assertThat(filtered.getGroupedFields()).contains(5);
        assertThat(filtered.getGroupedFields()).contains(1);
        assertThat(filtered.getOrdering()).isNull();
    }

    @Test
    void testGroupingErased() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"0;2"}, null, null, tupleInfo, tupleInfo);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setGroupedFields(new FieldSet(0, 2, 3));

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testOrderPreserved1() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"1;4;6"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(4, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setOrdering(o);

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getOrdering()).isNotNull();
        assertThat(filtered.getOrdering().getNumberOfFields()).isEqualTo(3);
        assertThat(filtered.getOrdering().getFieldNumber(0).intValue()).isEqualTo(4);
        assertThat(filtered.getOrdering().getFieldNumber(1).intValue()).isEqualTo(1);
        assertThat(filtered.getOrdering().getFieldNumber(2).intValue()).isEqualTo(6);
        assertThat(filtered.getOrdering().getType(0)).isEqualTo(LongValue.class);
        assertThat(filtered.getOrdering().getType(1)).isEqualTo(IntValue.class);
        assertThat(filtered.getOrdering().getType(2)).isEqualTo(ByteValue.class);
        assertThat(filtered.getOrdering().getOrder(0)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getOrdering().getOrder(1)).isEqualTo(Order.ASCENDING);
        assertThat(filtered.getOrdering().getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getGroupedFields()).isNull();
    }

    @Test
    void testOrderPreserved2() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"5->1;0->4;2->6"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(4, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setOrdering(o);

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getOrdering()).isNotNull();
        assertThat(filtered.getOrdering().getNumberOfFields()).isEqualTo(3);
        assertThat(filtered.getOrdering().getFieldNumber(0).intValue()).isEqualTo(0);
        assertThat(filtered.getOrdering().getFieldNumber(1).intValue()).isEqualTo(5);
        assertThat(filtered.getOrdering().getFieldNumber(2).intValue()).isEqualTo(2);
        assertThat(filtered.getOrdering().getType(0)).isEqualTo(LongValue.class);
        assertThat(filtered.getOrdering().getType(1)).isEqualTo(IntValue.class);
        assertThat(filtered.getOrdering().getType(2)).isEqualTo(ByteValue.class);
        assertThat(filtered.getOrdering().getOrder(0)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getOrdering().getOrder(1)).isEqualTo(Order.ASCENDING);
        assertThat(filtered.getOrdering().getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getGroupedFields()).isNull();
    }

    @Test
    void testOrderErased() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"1; 4"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(4, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setOrdering(o);

        RequestedLocalProperties filtered = rlProp.filterBySemanticProperties(sProps, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testDualGroupingPreserved() {

        DualInputSemanticProperties dprops = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dprops,
                new String[] {"1->0;3;2->4"},
                new String[] {"0->7;1"},
                null,
                null,
                null,
                null,
                tupleInfo,
                tupleInfo,
                tupleInfo);

        RequestedLocalProperties lprops1 = new RequestedLocalProperties();
        lprops1.setGroupedFields(new FieldSet(0, 3, 4));

        RequestedLocalProperties lprops2 = new RequestedLocalProperties();
        lprops2.setGroupedFields(new FieldSet(7, 1));

        RequestedLocalProperties filtered1 = lprops1.filterBySemanticProperties(dprops, 0);
        RequestedLocalProperties filtered2 = lprops2.filterBySemanticProperties(dprops, 1);

        assertThat(filtered1).isNotNull();
        assertThat(filtered1.getGroupedFields()).isNotNull();
        assertThat(filtered1.getGroupedFields()).hasSize(3);
        assertThat(filtered1.getGroupedFields()).contains(1);
        assertThat(filtered1.getGroupedFields()).contains(2);
        assertThat(filtered1.getGroupedFields()).contains(3);
        assertThat(filtered1.getOrdering()).isNull();

        assertThat(filtered2).isNotNull();
        assertThat(filtered2.getGroupedFields()).isNotNull();
        assertThat(filtered2.getGroupedFields()).hasSize(2);
        assertThat(filtered2.getGroupedFields()).contains(0);
        assertThat(filtered2.getGroupedFields()).contains(1);
        assertThat(filtered2.getOrdering()).isNull();
    }

    @Test
    void testInvalidInputIndex() {

        SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProps, new String[] {"1; 4"}, null, null, tupleInfo, tupleInfo);

        RequestedLocalProperties rlProp = new RequestedLocalProperties();
        rlProp.setGroupedFields(new FieldSet(1, 4));

        assertThatThrownBy(() -> rlProp.filterBySemanticProperties(sProps, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
