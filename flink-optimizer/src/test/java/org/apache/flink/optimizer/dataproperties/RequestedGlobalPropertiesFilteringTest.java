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

import org.apache.flink.api.common.distributions.DataDistribution;
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

public class RequestedGlobalPropertiesFilteringTest {

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

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 1, 2));

        assertThatThrownBy(() -> rgProps.filterBySemanticProperties(null, 0))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEraseAll1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 1, 2));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testEraseAll2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"3;4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 1, 2));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testHashPartitioningPreserved1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;3;4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setHashPartitioned(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.HASH_PARTITIONED);
        assertThat(filtered.getPartitionedFields()).isNotNull();
        assertThat(filtered.getPartitionedFields()).hasSize(3);
        assertThat(filtered.getPartitionedFields()).contains(0);
        assertThat(filtered.getPartitionedFields()).contains(3);
        assertThat(filtered.getPartitionedFields()).contains(4);
        assertThat(filtered.getDataDistribution()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
        assertThat(filtered.getOrdering()).isNull();
    }

    @Test
    void testHashPartitioningPreserved2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"2->0;1->3;7->4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setHashPartitioned(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.HASH_PARTITIONED);
        assertThat(filtered.getPartitionedFields()).isNotNull();
        assertThat(filtered.getPartitionedFields()).hasSize(3);
        assertThat(filtered.getPartitionedFields()).contains(1);
        assertThat(filtered.getPartitionedFields()).contains(2);
        assertThat(filtered.getPartitionedFields()).contains(7);
        assertThat(filtered.getDataDistribution()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
        assertThat(filtered.getOrdering()).isNull();
    }

    @Test
    void testHashPartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setHashPartitioned(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testAnyPartitioningPreserved1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;3;4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.ANY_PARTITIONING);
        assertThat(filtered.getPartitionedFields()).isNotNull();
        assertThat(filtered.getPartitionedFields()).hasSize(3);
        assertThat(filtered.getPartitionedFields()).contains(0);
        assertThat(filtered.getPartitionedFields()).contains(3);
        assertThat(filtered.getPartitionedFields()).contains(4);
        assertThat(filtered.getDataDistribution()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
        assertThat(filtered.getOrdering()).isNull();
    }

    @Test
    void testAnyPartitioningPreserved2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"2->0;1->3;7->4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.ANY_PARTITIONING);
        assertThat(filtered.getPartitionedFields()).isNotNull();
        assertThat(filtered.getPartitionedFields()).hasSize(3);
        assertThat(filtered.getPartitionedFields()).contains(1);
        assertThat(filtered.getPartitionedFields()).contains(2);
        assertThat(filtered.getPartitionedFields()).contains(7);
        assertThat(filtered.getDataDistribution()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
        assertThat(filtered.getOrdering()).isNull();
    }

    @Test
    void testAnyPartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testRangePartitioningPreserved1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"1;3;6"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(3, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setRangePartitioned(o);

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.RANGE_PARTITIONED);
        assertThat(filtered.getOrdering()).isNotNull();
        assertThat(filtered.getOrdering().getNumberOfFields()).isEqualTo(3);
        assertThat(filtered.getOrdering().getFieldNumber(0).intValue()).isEqualTo(3);
        assertThat(filtered.getOrdering().getFieldNumber(1).intValue()).isEqualTo(1);
        assertThat(filtered.getOrdering().getFieldNumber(2).intValue()).isEqualTo(6);
        assertThat(filtered.getOrdering().getType(0)).isEqualTo(LongValue.class);
        assertThat(filtered.getOrdering().getType(1)).isEqualTo(IntValue.class);
        assertThat(filtered.getOrdering().getType(2)).isEqualTo(ByteValue.class);
        assertThat(filtered.getOrdering().getOrder(0)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getOrdering().getOrder(1)).isEqualTo(Order.ASCENDING);
        assertThat(filtered.getOrdering().getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getPartitionedFields()).isNull();
        assertThat(filtered.getDataDistribution()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
    }

    @Test
    void testRangePartitioningPreserved2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"7->3;1->1;2->6"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(3, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setRangePartitioned(o);

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.RANGE_PARTITIONED);
        assertThat(filtered.getOrdering()).isNotNull();
        assertThat(filtered.getOrdering().getNumberOfFields()).isEqualTo(3);
        assertThat(filtered.getOrdering().getFieldNumber(0).intValue()).isEqualTo(7);
        assertThat(filtered.getOrdering().getFieldNumber(1).intValue()).isEqualTo(1);
        assertThat(filtered.getOrdering().getFieldNumber(2).intValue()).isEqualTo(2);
        assertThat(filtered.getOrdering().getType(0)).isEqualTo(LongValue.class);
        assertThat(filtered.getOrdering().getType(1)).isEqualTo(IntValue.class);
        assertThat(filtered.getOrdering().getType(2)).isEqualTo(ByteValue.class);
        assertThat(filtered.getOrdering().getOrder(0)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getOrdering().getOrder(1)).isEqualTo(Order.ASCENDING);
        assertThat(filtered.getOrdering().getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getPartitionedFields()).isNull();
        assertThat(filtered.getDataDistribution()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
    }

    @Test
    void testRangePartitioningPreserved3() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"7->3;1->1;2->6"}, null, null, tupleInfo, tupleInfo);

        DataDistribution dd = new MockDistribution();
        Ordering o = new Ordering();
        o.appendOrdering(3, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setRangePartitioned(o, dd);

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNotNull();
        assertThat(filtered.getPartitioning()).isEqualTo(PartitioningProperty.RANGE_PARTITIONED);
        assertThat(filtered.getOrdering()).isNotNull();
        assertThat(filtered.getOrdering().getNumberOfFields()).isEqualTo(3);
        assertThat(filtered.getOrdering().getFieldNumber(0).intValue()).isEqualTo(7);
        assertThat(filtered.getOrdering().getFieldNumber(1).intValue()).isEqualTo(1);
        assertThat(filtered.getOrdering().getFieldNumber(2).intValue()).isEqualTo(2);
        assertThat(filtered.getOrdering().getType(0)).isEqualTo(LongValue.class);
        assertThat(filtered.getOrdering().getType(1)).isEqualTo(IntValue.class);
        assertThat(filtered.getOrdering().getType(2)).isEqualTo(ByteValue.class);
        assertThat(filtered.getOrdering().getOrder(0)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getOrdering().getOrder(1)).isEqualTo(Order.ASCENDING);
        assertThat(filtered.getOrdering().getOrder(2)).isEqualTo(Order.DESCENDING);
        assertThat(filtered.getDataDistribution()).isNotNull();
        assertThat(filtered.getDataDistribution()).isEqualTo(dd);
        assertThat(filtered.getPartitionedFields()).isNull();
        assertThat(filtered.getCustomPartitioner()).isNull();
    }

    @Test
    void testRangePartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"1;2"}, null, null, tupleInfo, tupleInfo);

        Ordering o = new Ordering();
        o.appendOrdering(3, LongValue.class, Order.DESCENDING);
        o.appendOrdering(1, IntValue.class, Order.ASCENDING);
        o.appendOrdering(6, ByteValue.class, Order.DESCENDING);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setRangePartitioned(o);

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testCustomPartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setCustomPartitioned(new FieldSet(0, 1, 2), new MockPartitioner());

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testRandomDistributionErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setRandomPartitioning();

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testReplicationErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setFullyReplicated();

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testRebalancingErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setForceRebalancing();

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertThat(filtered).isNull();
    }

    @Test
    void testDualHashPartitioningPreserved() {

        DualInputSemanticProperties dprops = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dprops,
                new String[] {"0;2;4"},
                new String[] {"1->3;4->6;3->7"},
                null,
                null,
                null,
                null,
                tupleInfo,
                tupleInfo,
                tupleInfo);

        RequestedGlobalProperties gprops1 = new RequestedGlobalProperties();
        RequestedGlobalProperties gprops2 = new RequestedGlobalProperties();
        gprops1.setHashPartitioned(new FieldSet(2, 0, 4));
        gprops2.setHashPartitioned(new FieldSet(3, 6, 7));
        RequestedGlobalProperties filtered1 = gprops1.filterBySemanticProperties(dprops, 0);
        RequestedGlobalProperties filtered2 = gprops2.filterBySemanticProperties(dprops, 1);

        assertThat(filtered1).isNotNull();
        assertThat(filtered1.getPartitioning()).isEqualTo(PartitioningProperty.HASH_PARTITIONED);
        assertThat(filtered1.getPartitionedFields()).isNotNull();
        assertThat(filtered1.getPartitionedFields()).hasSize(3);
        assertThat(filtered1.getPartitionedFields()).contains(0);
        assertThat(filtered1.getPartitionedFields()).contains(2);
        assertThat(filtered1.getPartitionedFields()).contains(4);
        assertThat(filtered1.getOrdering()).isNull();
        assertThat(filtered1.getCustomPartitioner()).isNull();
        assertThat(filtered1.getDataDistribution()).isNull();

        assertThat(filtered2).isNotNull();
        assertThat(filtered2.getPartitioning()).isEqualTo(PartitioningProperty.HASH_PARTITIONED);
        assertThat(filtered2.getPartitionedFields()).isNotNull();
        assertThat(filtered2.getPartitionedFields()).hasSize(3);
        assertThat(filtered2.getPartitionedFields()).contains(1);
        assertThat(filtered2.getPartitionedFields()).contains(3);
        assertThat(filtered2.getPartitionedFields()).contains(4);
        assertThat(filtered2.getOrdering()).isNull();
        assertThat(filtered2.getCustomPartitioner()).isNull();
        assertThat(filtered2.getDataDistribution()).isNull();
    }

    @Test
    void testInvalidInputIndex() {
        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties gprops = new RequestedGlobalProperties();
        gprops.setHashPartitioned(new FieldSet(0, 1));

        assertThatThrownBy(() -> gprops.filterBySemanticProperties(sprops, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
