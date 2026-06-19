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

package org.apache.flink.util;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** This class contains tests for the {@link org.apache.flink.util.AbstractID} class. */
class AbstractIDTest {

    /** Tests the serialization/deserialization of an abstract ID. */
    @Test
    void testSerialization() throws Exception {
        final AbstractID origID = new AbstractID();
        final AbstractID copyID = CommonTestUtils.createCopySerializable(origID);

        assertThat(copyID).hasSameHashCodeAs(origID.hashCode());
        assertThat(copyID).isEqualTo(origID);
    }

    @Test
    void testConvertToBytes() {
        final AbstractID origID = new AbstractID();

        AbstractID copy1 = new AbstractID(origID);
        AbstractID copy2 = new AbstractID(origID.getBytes());
        AbstractID copy3 = new AbstractID(origID.getLowerPart(), origID.getUpperPart());

        assertThat(copy1).isEqualTo(origID);
        assertThat(copy2).isEqualTo(origID);
        assertThat(copy3).isEqualTo(origID);
    }

    @Test
    void testCompare() throws Exception {
        AbstractID id1 = new AbstractID(0, 0);
        AbstractID id2 = new AbstractID(1, 0);
        AbstractID id3 = new AbstractID(0, 1);
        AbstractID id4 = new AbstractID(-1, 0);
        AbstractID id5 = new AbstractID(0, -1);
        AbstractID id6 = new AbstractID(-1, -1);

        AbstractID id7 = new AbstractID(Long.MAX_VALUE, Long.MAX_VALUE);
        AbstractID id8 = new AbstractID(Long.MIN_VALUE, Long.MIN_VALUE);
        AbstractID id9 = new AbstractID(Long.MAX_VALUE, Long.MIN_VALUE);
        AbstractID id10 = new AbstractID(Long.MIN_VALUE, Long.MAX_VALUE);

        // test self equality
        assertThat(id1).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id1));
        assertThat(id2).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id2));
        assertThat(id3).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id3));
        assertThat(id4).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id4));
        assertThat(id5).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id5));
        assertThat(id6).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id6));
        assertThat(id7).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id7));
        assertThat(id8).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id8));
        assertThat(id9).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id9));
        assertThat(id10).isEqualByComparingTo(CommonTestUtils.createCopySerializable(id10));

        // test order
        assertCompare(id1, id2, -1);
        assertCompare(id1, id3, -1);
        assertCompare(id1, id4, 1);
        assertCompare(id1, id5, 1);
        assertCompare(id1, id6, 1);
        assertCompare(id2, id5, 1);
        assertCompare(id3, id5, 1);
        assertCompare(id2, id6, 1);
        assertCompare(id3, id6, 1);
        assertCompare(id1, id7, -1);
        assertCompare(id1, id8, 1);
        assertCompare(id7, id8, 1);
        assertCompare(id9, id10, -1);
        assertCompare(id7, id9, 1);
        assertCompare(id7, id10, 1);
        assertCompare(id8, id9, -1);
        assertCompare(id8, id10, -1);
    }

    /**
     * FLINK-10412 marks the {@link AbstractID#toString} field as transient. This tests ensures that
     * {@link AbstractID} which have been serialized with the toString field can still be
     * deserialized. For that purpose the files abstractID-with-toString-field and
     * abstractID-with-toString-field-set have been created with the serialized data.
     */
    @Test
    void testOldAbstractIDDeserialization() throws Exception {
        final long lowerPart = 42L;
        final long upperPart = 1337L;
        final AbstractID expectedAbstractId = new AbstractID(lowerPart, upperPart);

        final String resourceName1 = "abstractID-with-toString-field";
        try (final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream(resourceName1)) {
            final AbstractID deserializedAbstractId =
                    InstantiationUtil.deserializeObject(
                            resourceAsStream, getClass().getClassLoader());
            assertThat(deserializedAbstractId).isEqualTo(expectedAbstractId);
        }

        final String resourceName2 = "abstractID-with-toString-field-set";
        try (final InputStream resourceAsStream =
                getClass().getClassLoader().getResourceAsStream(resourceName2)) {
            final AbstractID deserializedAbstractId =
                    InstantiationUtil.deserializeObject(
                            resourceAsStream, getClass().getClassLoader());
            assertThat(deserializedAbstractId).isEqualTo(expectedAbstractId);
        }
    }

    private static void assertCompare(AbstractID a, AbstractID b, int signum) {
        int cmpAB = a.compareTo(b);
        int cmpBA = b.compareTo(a);

        int sgnAB = cmpAB > 0 ? 1 : (cmpAB < 0 ? -1 : 0);
        int sgnBA = cmpBA > 0 ? 1 : (cmpBA < 0 ? -1 : 0);

        assertThat(sgnAB).isEqualTo(signum);
        assertThat(-sgnBA).isEqualTo(sgnAB);
    }
}
