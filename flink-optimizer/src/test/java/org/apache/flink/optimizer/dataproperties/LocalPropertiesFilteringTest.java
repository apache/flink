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

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LocalPropertiesFilteringTest {

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
    void testAllErased1() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = LocalProperties.forGrouping(new FieldList(0, 1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(3, 4));
        lProps = lProps.addUniqueFields(new FieldSet(5, 6));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);

        assertThat(filtered.getGroupedFields()).isNull();
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testAllErased2() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"5"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = LocalProperties.forGrouping(new FieldList(0, 1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(3, 4));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);

        assertThat(filtered.getGroupedFields()).isNull();
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testGroupingPreserved1() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;2;3"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = LocalProperties.forGrouping(new FieldList(0, 2, 3));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);

        assertThat(filtered.getGroupedFields()).isNotNull();
        assertThat(filtered.getGroupedFields()).hasSize(3);
        assertThat(filtered.getGroupedFields()).contains(0);
        assertThat(filtered.getGroupedFields()).contains(2);
        assertThat(filtered.getGroupedFields()).contains(3);
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testGroupingPreserved2() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0->4;2->0;3->7"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = LocalProperties.forGrouping(new FieldList(0, 2, 3));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);

        assertThat(filtered.getGroupedFields()).isNotNull();
        assertThat(filtered.getGroupedFields()).hasSize(3);
        assertThat(filtered.getGroupedFields()).contains(4);
        assertThat(filtered.getGroupedFields()).contains(0);
        assertThat(filtered.getGroupedFields()).contains(7);
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testGroupingErased() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0->4;2->0"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = LocalProperties.forGrouping(new FieldList(0, 2, 3));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);

        assertThat(filtered.getGroupedFields()).isNull();
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testSortingPreserved1() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;2;5"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(2, IntValue.class, Order.ASCENDING);
        o.appendOrdering(0, StringValue.class, Order.DESCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        LocalProperties lProps = LocalProperties.forOrdering(o);

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldList gFields = filtered.getGroupedFields();
        Ordering order = filtered.getOrdering();

        assertThat(gFields).isNotNull();
        assertThat(gFields).hasSize(3);
        assertThat(gFields).contains(0);
        assertThat(gFields).contains(2);
        assertThat(gFields).contains(5);
        assertThat(order).isNotNull();
        assertThat(order.getNumberOfFields()).isEqualTo(3);
        assertThat(order.getFieldNumber(0).intValue()).isEqualTo(2);
        assertThat(order.getFieldNumber(1).intValue()).isEqualTo(0);
        assertThat(order.getFieldNumber(2).intValue()).isEqualTo(5);
        assertThat(order.getOrder(0)).isEqualTo(Order.ASCENDING);
        assertThat(order.getOrder(1)).isEqualTo(Order.DESCENDING);
        assertThat(order.getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(order.getType(0)).isEqualTo(IntValue.class);
        assertThat(order.getType(1)).isEqualTo(StringValue.class);
        assertThat(order.getType(2)).isEqualTo(LongValue.class);
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testSortingPreserved2() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0->3;2->7;5->1"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(2, IntValue.class, Order.ASCENDING);
        o.appendOrdering(0, StringValue.class, Order.DESCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        LocalProperties lProps = LocalProperties.forOrdering(o);

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldList gFields = filtered.getGroupedFields();
        Ordering order = filtered.getOrdering();

        assertThat(gFields).isNotNull();
        assertThat(gFields).hasSize(3);
        assertThat(gFields).contains(3);
        assertThat(gFields).contains(7);
        assertThat(gFields).contains(1);
        assertThat(order).isNotNull();
        assertThat(order.getNumberOfFields()).isEqualTo(3);
        assertThat(order.getFieldNumber(0).intValue()).isEqualTo(7);
        assertThat(order.getFieldNumber(1).intValue()).isEqualTo(3);
        assertThat(order.getFieldNumber(2).intValue()).isEqualTo(1);
        assertThat(order.getOrder(0)).isEqualTo(Order.ASCENDING);
        assertThat(order.getOrder(1)).isEqualTo(Order.DESCENDING);
        assertThat(order.getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(order.getType(0)).isEqualTo(IntValue.class);
        assertThat(order.getType(1)).isEqualTo(StringValue.class);
        assertThat(order.getType(2)).isEqualTo(LongValue.class);
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testSortingPreserved3() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;2"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(2, IntValue.class, Order.ASCENDING);
        o.appendOrdering(0, StringValue.class, Order.DESCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        LocalProperties lProps = LocalProperties.forOrdering(o);

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldList gFields = filtered.getGroupedFields();
        Ordering order = filtered.getOrdering();

        assertThat(gFields).isNotNull();
        assertThat(gFields).hasSize(2);
        assertThat(gFields).contains(0);
        assertThat(gFields).contains(2);
        assertThat(order).isNotNull();
        assertThat(order.getNumberOfFields()).isEqualTo(2);
        assertThat(order.getFieldNumber(0).intValue()).isEqualTo(2);
        assertThat(order.getFieldNumber(1).intValue()).isEqualTo(0);
        assertThat(order.getOrder(0)).isEqualTo(Order.ASCENDING);
        assertThat(order.getOrder(1)).isEqualTo(Order.DESCENDING);
        assertThat(order.getType(0)).isEqualTo(IntValue.class);
        assertThat(order.getType(1)).isEqualTo(StringValue.class);
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testSortingPreserved4() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"2->7;5"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(2, IntValue.class, Order.ASCENDING);
        o.appendOrdering(0, StringValue.class, Order.DESCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        LocalProperties lProps = LocalProperties.forOrdering(o);

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldList gFields = filtered.getGroupedFields();
        Ordering order = filtered.getOrdering();

        assertThat(gFields).isNotNull();
        assertThat(gFields).hasSize(1);
        assertThat(gFields).contains(7);
        assertThat(order).isNotNull();
        assertThat(order.getNumberOfFields()).isEqualTo(1);
        assertThat(order.getFieldNumber(0).intValue()).isEqualTo(7);
        assertThat(order.getOrder(0)).isEqualTo(Order.ASCENDING);
        assertThat(order.getType(0)).isEqualTo(IntValue.class);
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testSortingErased() {
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;5"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(2, IntValue.class, Order.ASCENDING);
        o.appendOrdering(0, StringValue.class, Order.DESCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        LocalProperties lProps = LocalProperties.forOrdering(o);

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldList gFields = filtered.getGroupedFields();
        Ordering order = filtered.getOrdering();

        assertThat(gFields).isNull();
        assertThat(order).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testUniqueFieldsPreserved1() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;1;2;3;4"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = new LocalProperties();
        lProps = lProps.addUniqueFields(new FieldSet(0, 1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(3, 4));
        lProps = lProps.addUniqueFields(new FieldSet(4, 5, 6));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldSet expected1 = new FieldSet(0, 1, 2);
        FieldSet expected2 = new FieldSet(3, 4);

        assertThat(filtered.getGroupedFields()).isNull();
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNotNull();
        assertThat(filtered.getUniqueFields()).hasSize(2);
        assertThat(filtered.getUniqueFields()).contains(expected1);
        assertThat(filtered.getUniqueFields()).contains(expected2);
    }

    @Test
    void testUniqueFieldsPreserved2() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;1;2;3;4"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = LocalProperties.forGrouping(new FieldList(1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(0, 1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(3, 4));
        lProps = lProps.addUniqueFields(new FieldSet(4, 5, 6));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldSet expected1 = new FieldSet(0, 1, 2);
        FieldSet expected2 = new FieldSet(3, 4);

        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getGroupedFields()).isNotNull();
        assertThat(filtered.getGroupedFields()).hasSize(2);
        assertThat(filtered.getGroupedFields()).contains(1);
        assertThat(filtered.getGroupedFields()).contains(2);
        assertThat(filtered.getUniqueFields()).isNotNull();
        assertThat(filtered.getUniqueFields()).hasSize(2);
        assertThat(filtered.getUniqueFields()).contains(expected1);
        assertThat(filtered.getUniqueFields()).contains(expected2);
    }

    @Test
    void testUniqueFieldsPreserved3() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0->7;1->6;2->5;3->4;4->3"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = new LocalProperties();
        lProps = lProps.addUniqueFields(new FieldSet(0, 1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(3, 4));
        lProps = lProps.addUniqueFields(new FieldSet(4, 5, 6));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);
        FieldSet expected1 = new FieldSet(5, 6, 7);
        FieldSet expected2 = new FieldSet(3, 4);

        assertThat(filtered.getGroupedFields()).isNull();
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNotNull();
        assertThat(filtered.getUniqueFields()).hasSize(2);
        assertThat(filtered.getUniqueFields()).contains(expected1);
        assertThat(filtered.getUniqueFields()).contains(expected2);
    }

    @Test
    void testUniqueFieldsErased() {

        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, new String[] {"0;1;4"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lProps = new LocalProperties();
        lProps = lProps.addUniqueFields(new FieldSet(0, 1, 2));
        lProps = lProps.addUniqueFields(new FieldSet(3, 4));
        lProps = lProps.addUniqueFields(new FieldSet(4, 5, 6));

        LocalProperties filtered = lProps.filterBySemanticProperties(sp, 0);

        assertThat(filtered.getGroupedFields()).isNull();
        assertThat(filtered.getOrdering()).isNull();
        assertThat(filtered.getUniqueFields()).isNull();
    }

    @Test
    void testInvalidInputIndex() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        LocalProperties lprops = LocalProperties.forGrouping(new FieldList(0, 1));

        assertThatThrownBy(() -> lprops.filterBySemanticProperties(sprops, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
