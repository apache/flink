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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.test.operators.util.CollectionDataSets;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

/** Test TypeInfo serializer tree. */
public class GenericTypeInfoTest {

    @Test
    public void testSerializerTree() {
        @SuppressWarnings("unchecked")
        TypeInformation<CollectionDataSets.PojoWithCollectionGeneric> ti =
                (TypeInformation<CollectionDataSets.PojoWithCollectionGeneric>)
                        TypeExtractor.createTypeInfo(
                                CollectionDataSets.PojoWithCollectionGeneric.class);

        final String serTree =
                Utils.getSerializerTree(ti)
                        // normalize String/BigInteger representations as they vary across java
                        // versions
                        // do 2 passes for BigInteger since they occur at different indentations
                        .replaceAll("(java\\.lang\\.String\\R)( {12}\\S*\\R)+", "$1")
                        .replaceAll(
                                "( {4}[a-zA-Z]+:java\\.math\\.BigInteger\\R)( {8}\\S*\\R)+", "$1")
                        .replaceAll(
                                "( {8}[a-zA-Z]+:java\\.math\\.BigInteger\\R)( {12}\\S*\\R)+", "$1");

        Assert.assertThat(
                Arrays.asList(serTree.split("\n")),
                containsInAnyOrder(
                        Arrays.asList(
                                        ("GenericTypeInfo (PojoWithCollectionGeneric)\n"
                                                        + "    pojos:java.util.List\n"
                                                        + "    key:int\n"
                                                        + "    sqlDate:java.sql.Date\n"
                                                        + "    bigInt:java.math.BigInteger\n"
                                                        + "    bigDecimalKeepItNull:java.math.BigDecimal\n"
                                                        + "        intVal:java.math.BigInteger\n"
                                                        + "        scale:int\n"
                                                        + "    scalaBigInt:scala.math.BigInt\n"
                                                        + "        bigInteger:java.math.BigInteger\n"
                                                        + "    mixed:java.util.List\n"
                                                        + "    makeMeGeneric:org.apache.flink.test.operators.util.CollectionDataSets$PojoWithDateAndEnum\n"
                                                        + "        group:java.lang.String\n"
                                                        + "        date:java.util.Date\n"
                                                        + "        cat:org.apache.flink.test.operators.util.CollectionDataSets$Category (is enum)\n")
                                                .split("\n"))
                                .toArray()));
    }
}
