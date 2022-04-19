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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GlobalPropertiesFilteringTest {

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

        SingleInputSemanticProperties semProps = new SingleInputSemanticProperties();

        GlobalProperties gprops = new GlobalProperties();
        gprops.setHashPartitioned(new FieldList(0, 1));
        gprops.addUniqueFieldCombination(new FieldSet(3, 4));
        gprops.addUniqueFieldCombination(new FieldSet(5, 6));

        GlobalProperties result = gprops.filterBySemanticProperties(semProps, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANDOM_PARTITIONED);
        assertThat(result.getPartitioningFields()).isNull();
        assertThat(result.getPartitioningOrdering()).isNull();
        assertThat(result.getUniqueFieldCombination()).isNull();
    }

    @Test
    void testAllErased2() {

        SingleInputSemanticProperties semProps = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                semProps, new String[] {"2"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setHashPartitioned(new FieldList(0, 1));
        gprops.addUniqueFieldCombination(new FieldSet(3, 4));
        gprops.addUniqueFieldCombination(new FieldSet(5, 6));

        GlobalProperties result = gprops.filterBySemanticProperties(semProps, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANDOM_PARTITIONED);
        assertThat(result.getPartitioningFields()).isNull();
        assertThat(result.getPartitioningOrdering()).isNull();
        assertThat(result.getUniqueFieldCombination()).isNull();
    }

    @Test
    void testHashPartitioningPreserved1() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1;4"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setHashPartitioned(new FieldList(0, 1, 4));

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.HASH_PARTITIONED);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(3);
        assertThat(pFields).contains(0);
        assertThat(pFields).contains(1);
        assertThat(pFields).contains(4);
    }

    @Test
    void testHashPartitioningPreserved2() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setHashPartitioned(new FieldList(0, 1, 4));

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.HASH_PARTITIONED);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(3);
        assertThat(pFields).contains(1);
        assertThat(pFields).contains(2);
        assertThat(pFields).contains(3);
    }

    @Test
    void testHashPartitioningErased() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setHashPartitioned(new FieldList(0, 1, 4));

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANDOM_PARTITIONED);
        assertThat(result.getPartitioningFields()).isNull();
    }

    @Test
    void testAnyPartitioningPreserved1() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1;4"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setAnyPartitioning(new FieldList(0, 1, 4));

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.ANY_PARTITIONING);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(3);
        assertThat(pFields).contains(0);
        assertThat(pFields).contains(1);
        assertThat(pFields).contains(4);
    }

    @Test
    void testAnyPartitioningPreserved2() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setAnyPartitioning(new FieldList(0, 1, 4));

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.ANY_PARTITIONING);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(3);
        assertThat(pFields).contains(1);
        assertThat(pFields).contains(2);
        assertThat(pFields).contains(3);
    }

    @Test
    void testAnyPartitioningErased() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setAnyPartitioning(new FieldList(0, 1, 4));

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANDOM_PARTITIONED);
        assertThat(result.getPartitioningFields()).isNull();
    }

    @Test
    void testCustomPartitioningPreserved1() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1;4"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        Partitioner<Tuple2<Long, Integer>> myP = new MockPartitioner();
        gprops.setCustomPartitioned(new FieldList(0, 4), myP);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.CUSTOM_PARTITIONING);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(2);
        assertThat(pFields).contains(0);
        assertThat(pFields).contains(4);
        assertThat(result.getCustomPartitioner()).isEqualTo(myP);
    }

    @Test
    void testCustomPartitioningPreserved2() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        Partitioner<Tuple2<Long, Integer>> myP = new MockPartitioner();
        gprops.setCustomPartitioned(new FieldList(0, 4), myP);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.CUSTOM_PARTITIONING);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(2);
        assertThat(pFields).contains(1);
        assertThat(pFields).contains(3);
        assertThat(result.getCustomPartitioner()).isEqualTo(myP);
    }

    @Test
    void testCustomPartitioningErased() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        Partitioner<Tuple2<Long, Integer>> myP = new MockPartitioner();
        gprops.setCustomPartitioned(new FieldList(0, 4), myP);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANDOM_PARTITIONED);
        assertThat(result.getPartitioningFields()).isNull();
        assertThat(result.getCustomPartitioner()).isNull();
    }

    @Test
    void testRangePartitioningPreserved1() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"1;2;5"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        o.appendOrdering(2, StringValue.class, Order.ASCENDING);
        GlobalProperties gprops = new GlobalProperties();
        gprops.setRangePartitioned(o);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANGE_PARTITIONED);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(3);
        assertThat(pFields.get(0).intValue()).isEqualTo(1);
        assertThat(pFields.get(1).intValue()).isEqualTo(5);
        assertThat(pFields.get(2).intValue()).isEqualTo(2);
        Ordering pOrder = result.getPartitioningOrdering();
        assertThat(pOrder.getNumberOfFields()).isEqualTo(3);
        assertThat(pOrder.getFieldNumber(0).intValue()).isEqualTo(1);
        assertThat(pOrder.getFieldNumber(1).intValue()).isEqualTo(5);
        assertThat(pOrder.getFieldNumber(2).intValue()).isEqualTo(2);
        assertThat(pOrder.getOrder(0)).isEqualTo(Order.ASCENDING);
        assertThat(pOrder.getOrder(1)).isEqualTo(Order.DESCENDING);
        assertThat(pOrder.getOrder(2)).isEqualTo(Order.ASCENDING);
        assertThat(pOrder.getType(0)).isEqualTo(IntValue.class);
        assertThat(pOrder.getType(1)).isEqualTo(LongValue.class);
        assertThat(pOrder.getType(2)).isEqualTo(StringValue.class);
    }

    @Test
    void testRangePartitioningPreserved2() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"1->3; 2->0; 5->1"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        o.appendOrdering(2, StringValue.class, Order.ASCENDING);
        GlobalProperties gprops = new GlobalProperties();
        gprops.setRangePartitioned(o);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANGE_PARTITIONED);
        FieldList pFields = result.getPartitioningFields();
        assertThat(pFields).hasSize(3);
        assertThat(pFields.get(0).intValue()).isEqualTo(3);
        assertThat(pFields.get(1).intValue()).isEqualTo(1);
        assertThat(pFields.get(2).intValue()).isEqualTo(0);
        Ordering pOrder = result.getPartitioningOrdering();
        assertThat(pOrder.getNumberOfFields()).isEqualTo(3);
        assertThat(pOrder.getFieldNumber(0).intValue()).isEqualTo(3);
        assertThat(pOrder.getFieldNumber(1).intValue()).isEqualTo(1);
        assertThat(pOrder.getFieldNumber(2).intValue()).isEqualTo(0);
        assertThat(pOrder.getOrder(0)).isEqualTo(Order.ASCENDING);
        assertThat(pOrder.getOrder(1)).isEqualTo(Order.DESCENDING);
        assertThat(pOrder.getOrder(2)).isEqualTo(Order.ASCENDING);
        assertThat(pOrder.getType(0)).isEqualTo(IntValue.class);
        assertThat(pOrder.getType(1)).isEqualTo(LongValue.class);
        assertThat(pOrder.getType(2)).isEqualTo(StringValue.class);
    }

    @Test
    void testRangePartitioningErased() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"1;5"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(5, LongValue.class, Order.DESCENDING);
        o.appendOrdering(2, StringValue.class, Order.ASCENDING);
        GlobalProperties gprops = new GlobalProperties();
        gprops.setRangePartitioned(o);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.RANDOM_PARTITIONED);
        assertThat(result.getPartitioningOrdering()).isNull();
        assertThat(result.getPartitioningFields()).isNull();
    }

    @Test
    void testRebalancingPreserved() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0->1; 1->2; 4->3"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setForcedRebalanced();

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);

        assertThat(result.getPartitioning()).isEqualTo(PartitioningProperty.FORCED_REBALANCED);
        assertThat(result.getPartitioningFields()).isNull();
    }

    @Test
    void testUniqueFieldGroupsPreserved1() {
        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1;2;3;4"}, null, null, tupleInfo, tupleInfo);

        FieldSet set1 = new FieldSet(0, 1, 2);
        FieldSet set2 = new FieldSet(3, 4);
        FieldSet set3 = new FieldSet(4, 5, 6, 7);
        GlobalProperties gprops = new GlobalProperties();
        gprops.addUniqueFieldCombination(set1);
        gprops.addUniqueFieldCombination(set2);
        gprops.addUniqueFieldCombination(set3);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
        Set<FieldSet> unique = result.getUniqueFieldCombination();
        FieldSet expected1 = new FieldSet(0, 1, 2);
        FieldSet expected2 = new FieldSet(3, 4);

        assertThat(unique).hasSize(2);
        assertThat(unique).contains(expected1);
        assertThat(unique).contains(expected2);
    }

    @Test
    void testUniqueFieldGroupsPreserved2() {
        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0->5;1;2;3->6;4"}, null, null, tupleInfo, tupleInfo);

        FieldSet set1 = new FieldSet(0, 1, 2);
        FieldSet set2 = new FieldSet(3, 4);
        FieldSet set3 = new FieldSet(4, 5, 6, 7);
        GlobalProperties gprops = new GlobalProperties();
        gprops.addUniqueFieldCombination(set1);
        gprops.addUniqueFieldCombination(set2);
        gprops.addUniqueFieldCombination(set3);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
        Set<FieldSet> unique = result.getUniqueFieldCombination();
        FieldSet expected1 = new FieldSet(1, 2, 5);
        FieldSet expected2 = new FieldSet(4, 6);

        assertThat(unique).hasSize(2);
        assertThat(unique).contains(expected1);
        assertThat(unique).contains(expected2);
    }

    @Test
    void testUniqueFieldGroupsErased() {
        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0; 3; 5; 6; 7"}, null, null, tupleInfo, tupleInfo);

        FieldSet set1 = new FieldSet(0, 1, 2);
        FieldSet set2 = new FieldSet(3, 4);
        FieldSet set3 = new FieldSet(4, 5, 6, 7);
        GlobalProperties gprops = new GlobalProperties();
        gprops.addUniqueFieldCombination(set1);
        gprops.addUniqueFieldCombination(set2);
        gprops.addUniqueFieldCombination(set3);

        GlobalProperties result = gprops.filterBySemanticProperties(sprops, 0);
        assertThat(result.getUniqueFieldCombination()).isNull();
    }

    @Test
    void testInvalidInputIndex() {

        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        GlobalProperties gprops = new GlobalProperties();
        gprops.setHashPartitioned(new FieldList(0, 1));

        assertThatThrownBy(() -> gprops.filterBySemanticProperties(sprops, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
