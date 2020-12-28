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

package org.apache.flink.connector.hbase1;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.Registration;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.HBase;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Test case for {@link HBase} descriptor. */
public class HBaseDescriptorTest extends DescriptorTestBase {

    @Override
    protected List<Descriptor> descriptors() {
        HBase hbaseDesc0 =
                new HBase()
                        .version("1.4.3")
                        .tableName("testNs:table0")
                        .zookeeperQuorum("localhost:2181,localhost:2182,localhost:2183")
                        .zookeeperNodeParent("/hbase/root-dir");

        HBase hbaseDesc1 =
                new HBase()
                        .version("1.4.3")
                        .tableName("testNs:table1")
                        .zookeeperQuorum("localhost:2181")
                        .zookeeperNodeParent("/hbase/root")
                        .writeBufferFlushInterval("2s")
                        .writeBufferFlushMaxRows(100)
                        .writeBufferFlushMaxSize("1mb");

        return Arrays.asList(hbaseDesc0, hbaseDesc1);
    }

    @Override
    protected List<Map<String, String>> properties() {
        Map<String, String> prop0 = new HashMap<>();
        prop0.put("connector.version", "1.4.3");
        prop0.put("connector.type", "hbase");
        prop0.put("connector.table-name", "testNs:table0");
        prop0.put("connector.zookeeper.quorum", "localhost:2181,localhost:2182,localhost:2183");
        prop0.put("connector.zookeeper.znode.parent", "/hbase/root-dir");
        prop0.put("connector.property-version", "1");

        Map<String, String> prop1 = new HashMap<>();
        prop1.put("connector.version", "1.4.3");
        prop1.put("connector.type", "hbase");
        prop1.put("connector.table-name", "testNs:table1");
        prop1.put("connector.zookeeper.quorum", "localhost:2181");
        prop1.put("connector.zookeeper.znode.parent", "/hbase/root");
        prop1.put("connector.property-version", "1");
        prop1.put("connector.write.buffer-flush.interval", "2s");
        prop1.put("connector.write.buffer-flush.max-rows", "100");
        prop1.put("connector.write.buffer-flush.max-size", "1 mb");

        return Arrays.asList(prop0, prop1);
    }

    @Override
    protected DescriptorValidator validator() {
        return new HBaseValidator();
    }

    @Test
    public void testRequiredFields() {
        HBase hbaseDesc0 = new HBase();
        HBase hbaseDesc1 =
                new HBase()
                        .version("1.4.3")
                        .zookeeperQuorum("localhost:2181")
                        .zookeeperNodeParent("/hbase/root"); // no table name
        HBase hbaseDesc2 =
                new HBase()
                        .version("1.4.3")
                        .tableName("ns:table")
                        .zookeeperNodeParent("/hbase/root"); // no zookeeper quorum
        HBase hbaseDesc3 =
                new HBase().tableName("ns:table").zookeeperQuorum("localhost:2181"); // no version

        HBase[] testCases = new HBase[] {hbaseDesc0, hbaseDesc1, hbaseDesc2, hbaseDesc3};
        for (int i = 0; i < testCases.length; i++) {
            HBase hbaseDesc = testCases[i];
            DescriptorProperties properties = new DescriptorProperties();
            properties.putProperties(hbaseDesc.toProperties());
            boolean caughtExpectedException = false;
            try {
                validator().validate(properties);
            } catch (ValidationException e) {
                caughtExpectedException = true;
            }
            Assert.assertTrue(
                    "The case#" + i + " didn't get the expected error", caughtExpectedException);
        }
    }

    @Test
    public void testFormatNeed() {
        String expected =
                "The connector org.apache.flink.table.descriptors.HBase does not require a format description but org.apache.flink.connector.hbase1.HBaseDescriptorTest$1 found.";
        AtomicReference<CatalogTableImpl> reference = new AtomicReference<>();
        HBase hBase = new HBase();
        Registration registration = (path, table) -> reference.set((CatalogTableImpl) table);
        ConnectTableDescriptor descriptor =
                new StreamTableDescriptor(registration, hBase)
                        .withFormat(
                                new FormatDescriptor("myFormat", 1) {
                                    @Override
                                    protected Map<String, String> toFormatProperties() {
                                        return new HashMap<>();
                                    }
                                })
                        .withSchema(
                                new Schema()
                                        .field("f0", DataTypes.INT())
                                        .rowtime(new Rowtime().timestampsFromField("f0")));
        String actual = null;
        try {
            descriptor.toProperties();
        } catch (Exception e) {
            actual = e.getMessage();
        }
        Assert.assertEquals(expected, actual);
    }
}
