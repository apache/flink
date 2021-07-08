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

import org.junit.Test;

import static org.junit.Assert.*;

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

    @Test(expected = NullPointerException.class)
    public void testNullProps() {

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 1, 2));

        rgProps.filterBySemanticProperties(null, 0);
    }

    @Test
    public void testEraseAll1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 1, 2));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testEraseAll2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"3;4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 1, 2));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testHashPartitioningPreserved1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;3;4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setHashPartitioned(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.HASH_PARTITIONED, filtered.getPartitioning());
        assertNotNull(filtered.getPartitionedFields());
        assertEquals(3, filtered.getPartitionedFields().size());
        assertTrue(filtered.getPartitionedFields().contains(0));
        assertTrue(filtered.getPartitionedFields().contains(3));
        assertTrue(filtered.getPartitionedFields().contains(4));
        assertNull(filtered.getDataDistribution());
        assertNull(filtered.getCustomPartitioner());
        assertNull(filtered.getOrdering());
    }

    @Test
    public void testHashPartitioningPreserved2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"2->0;1->3;7->4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setHashPartitioned(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.HASH_PARTITIONED, filtered.getPartitioning());
        assertNotNull(filtered.getPartitionedFields());
        assertEquals(3, filtered.getPartitionedFields().size());
        assertTrue(filtered.getPartitionedFields().contains(1));
        assertTrue(filtered.getPartitionedFields().contains(2));
        assertTrue(filtered.getPartitionedFields().contains(7));
        assertNull(filtered.getDataDistribution());
        assertNull(filtered.getCustomPartitioner());
        assertNull(filtered.getOrdering());
    }

    @Test
    public void testHashPartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setHashPartitioned(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testAnyPartitioningPreserved1() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;3;4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.ANY_PARTITIONING, filtered.getPartitioning());
        assertNotNull(filtered.getPartitionedFields());
        assertEquals(3, filtered.getPartitionedFields().size());
        assertTrue(filtered.getPartitionedFields().contains(0));
        assertTrue(filtered.getPartitionedFields().contains(3));
        assertTrue(filtered.getPartitionedFields().contains(4));
        assertNull(filtered.getDataDistribution());
        assertNull(filtered.getCustomPartitioner());
        assertNull(filtered.getOrdering());
    }

    @Test
    public void testAnyPartitioningPreserved2() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"2->0;1->3;7->4"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.ANY_PARTITIONING, filtered.getPartitioning());
        assertNotNull(filtered.getPartitionedFields());
        assertEquals(3, filtered.getPartitionedFields().size());
        assertTrue(filtered.getPartitionedFields().contains(1));
        assertTrue(filtered.getPartitionedFields().contains(2));
        assertTrue(filtered.getPartitionedFields().contains(7));
        assertNull(filtered.getDataDistribution());
        assertNull(filtered.getCustomPartitioner());
        assertNull(filtered.getOrdering());
    }

    @Test
    public void testAnyPartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setAnyPartitioning(new FieldSet(0, 3, 4));

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testRangePartitioningPreserved1() {

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

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.RANGE_PARTITIONED, filtered.getPartitioning());
        assertNotNull(filtered.getOrdering());
        assertEquals(3, filtered.getOrdering().getNumberOfFields());
        assertEquals(3, filtered.getOrdering().getFieldNumber(0).intValue());
        assertEquals(1, filtered.getOrdering().getFieldNumber(1).intValue());
        assertEquals(6, filtered.getOrdering().getFieldNumber(2).intValue());
        assertEquals(LongValue.class, filtered.getOrdering().getType(0));
        assertEquals(IntValue.class, filtered.getOrdering().getType(1));
        assertEquals(ByteValue.class, filtered.getOrdering().getType(2));
        assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(0));
        assertEquals(Order.ASCENDING, filtered.getOrdering().getOrder(1));
        assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(2));
        assertNull(filtered.getPartitionedFields());
        assertNull(filtered.getDataDistribution());
        assertNull(filtered.getCustomPartitioner());
    }

    @Test
    public void testRangePartitioningPreserved2() {

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

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.RANGE_PARTITIONED, filtered.getPartitioning());
        assertNotNull(filtered.getOrdering());
        assertEquals(3, filtered.getOrdering().getNumberOfFields());
        assertEquals(7, filtered.getOrdering().getFieldNumber(0).intValue());
        assertEquals(1, filtered.getOrdering().getFieldNumber(1).intValue());
        assertEquals(2, filtered.getOrdering().getFieldNumber(2).intValue());
        assertEquals(LongValue.class, filtered.getOrdering().getType(0));
        assertEquals(IntValue.class, filtered.getOrdering().getType(1));
        assertEquals(ByteValue.class, filtered.getOrdering().getType(2));
        assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(0));
        assertEquals(Order.ASCENDING, filtered.getOrdering().getOrder(1));
        assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(2));
        assertNull(filtered.getPartitionedFields());
        assertNull(filtered.getDataDistribution());
        assertNull(filtered.getCustomPartitioner());
    }

    @Test
    public void testRangePartitioningPreserved3() {

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

        assertNotNull(filtered);
        assertEquals(PartitioningProperty.RANGE_PARTITIONED, filtered.getPartitioning());
        assertNotNull(filtered.getOrdering());
        assertEquals(3, filtered.getOrdering().getNumberOfFields());
        assertEquals(7, filtered.getOrdering().getFieldNumber(0).intValue());
        assertEquals(1, filtered.getOrdering().getFieldNumber(1).intValue());
        assertEquals(2, filtered.getOrdering().getFieldNumber(2).intValue());
        assertEquals(LongValue.class, filtered.getOrdering().getType(0));
        assertEquals(IntValue.class, filtered.getOrdering().getType(1));
        assertEquals(ByteValue.class, filtered.getOrdering().getType(2));
        assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(0));
        assertEquals(Order.ASCENDING, filtered.getOrdering().getOrder(1));
        assertEquals(Order.DESCENDING, filtered.getOrdering().getOrder(2));
        assertNotNull(filtered.getDataDistribution());
        assertEquals(dd, filtered.getDataDistribution());
        assertNull(filtered.getPartitionedFields());
        assertNull(filtered.getCustomPartitioner());
    }

    @Test
    public void testRangePartitioningErased() {

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

        assertNull(filtered);
    }

    @Test
    public void testCustomPartitioningErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setCustomPartitioned(new FieldSet(0, 1, 2), new MockPartitioner());

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testRandomDistributionErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setRandomPartitioning();

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testReplicationErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setFullyReplicated();

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testRebalancingErased() {

        SingleInputSemanticProperties sProp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sProp, new String[] {"0;1;2"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties rgProps = new RequestedGlobalProperties();
        rgProps.setForceRebalancing();

        RequestedGlobalProperties filtered = rgProps.filterBySemanticProperties(sProp, 0);

        assertNull(filtered);
    }

    @Test
    public void testDualHashPartitioningPreserved() {

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

        assertNotNull(filtered1);
        assertEquals(PartitioningProperty.HASH_PARTITIONED, filtered1.getPartitioning());
        assertNotNull(filtered1.getPartitionedFields());
        assertEquals(3, filtered1.getPartitionedFields().size());
        assertTrue(filtered1.getPartitionedFields().contains(0));
        assertTrue(filtered1.getPartitionedFields().contains(2));
        assertTrue(filtered1.getPartitionedFields().contains(4));
        assertNull(filtered1.getOrdering());
        assertNull(filtered1.getCustomPartitioner());
        assertNull(filtered1.getDataDistribution());

        assertNotNull(filtered2);
        assertEquals(PartitioningProperty.HASH_PARTITIONED, filtered2.getPartitioning());
        assertNotNull(filtered2.getPartitionedFields());
        assertEquals(3, filtered2.getPartitionedFields().size());
        assertTrue(filtered2.getPartitionedFields().contains(1));
        assertTrue(filtered2.getPartitionedFields().contains(3));
        assertTrue(filtered2.getPartitionedFields().contains(4));
        assertNull(filtered2.getOrdering());
        assertNull(filtered2.getCustomPartitioner());
        assertNull(filtered2.getDataDistribution());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testInvalidInputIndex() {
        SingleInputSemanticProperties sprops = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sprops, new String[] {"0;1"}, null, null, tupleInfo, tupleInfo);

        RequestedGlobalProperties gprops = new RequestedGlobalProperties();
        gprops.setHashPartitioned(new FieldSet(0, 1));

        gprops.filterBySemanticProperties(sprops, 1);
    }
}
