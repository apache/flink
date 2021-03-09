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

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

/** Test for {@link PojoTypeInfo}. */
public class PojoTypeInfoTest extends TypeInformationTestBase<PojoTypeInfo<?>> {

    @Override
    protected PojoTypeInfo<?>[] getTestData() {
        return new PojoTypeInfo<?>[] {
            (PojoTypeInfo<?>) TypeExtractor.getForClass(TestPojo.class),
            (PojoTypeInfo<?>) TypeExtractor.getForClass(AlternatePojo.class),
            (PojoTypeInfo<?>) TypeExtractor.getForClass(PrimitivePojo.class),
            (PojoTypeInfo<?>) TypeExtractor.getForClass(UnderscorePojo.class)
        };
    }

    public static final class TestPojo {

        public int someInt;

        private String aString;

        public Double[] doubleArray;

        public void setaString(String aString) {
            this.aString = aString;
        }

        public String getaString() {
            return aString;
        }
    }

    public static final class AlternatePojo {

        public int someInt;

        private String aString;

        public Double[] doubleArray;

        public void setaString(String aString) {
            this.aString = aString;
        }

        public String getaString() {
            return aString;
        }
    }

    public static final class PrimitivePojo {

        private int someInt;

        public void setSomeInt(Integer someInt) {
            this.someInt = someInt;
        }

        public Integer getSomeInt() {
            return this.someInt;
        }
    }

    public static final class UnderscorePojo {

        private int some_int;

        public void setSomeInt(int some_int) {
            this.some_int = some_int;
        }

        public Integer getSomeInt() {
            return this.some_int;
        }
    }
}
