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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PojoTypeInformationTest {

    public static class SimplePojo {
        public String str;
        public Boolean Bl;
        public boolean bl;
        public Byte Bt;
        public byte bt;
        public Short Shrt;
        public short shrt;
        public Integer Intgr;
        public int intgr;
        public Long Lng;
        public long lng;
        public Float Flt;
        public float flt;
        public Double Dbl;
        public double dbl;
        public Character Ch;
        public char ch;
        public int[] primIntArray;
        public Integer[] intWrapperArray;
    }

    @Test
    void testSimplePojoTypeExtraction() {
        TypeInformation<SimplePojo> type = TypeExtractor.getForClass(SimplePojo.class);
        assertThat(type).isInstanceOf(CompositeType.class);
    }

    public static class NestedPojoInner {
        public String field;
    }

    public static class NestedPojoOuter {
        public Integer intField;
        public NestedPojoInner inner;
    }

    @Test
    void testNestedPojoTypeExtraction() {
        TypeInformation<NestedPojoOuter> type = TypeExtractor.getForClass(NestedPojoOuter.class);
        assertThat(type).isInstanceOf(CompositeType.class);
    }

    public static class Recursive1Pojo {
        public Integer intField;
        public Recursive2Pojo rec;
    }

    public static class Recursive2Pojo {
        public String strField;
        public Recursive1Pojo rec;
    }

    @Test
    void testRecursivePojoTypeExtraction() {
        // This one tests whether a recursive pojo is detected using the set of visited
        // types in the type extractor. The recursive field will be handled using the generic
        // serializer.
        TypeInformation<Recursive1Pojo> type = TypeExtractor.getForClass(Recursive1Pojo.class);
        assertThat(type).isInstanceOf(CompositeType.class);
    }

    @Test
    void testRecursivePojoObjectTypeExtraction() {
        TypeInformation<Recursive1Pojo> type = TypeExtractor.getForObject(new Recursive1Pojo());
        assertThat(type).isInstanceOf(CompositeType.class);
    }
}
