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

package org.apache.flink.api.common.serialization;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TypeInformationSerializationSchema}. */
class TypeInformationSerializationSchemaTest {

    @Test
    void testDeSerialization() {
        TypeInformation<MyPOJO> info = TypeExtractor.getForClass(MyPOJO.class);

        TypeInformationSerializationSchema<MyPOJO> schema =
                new TypeInformationSerializationSchema<>(info, new ExecutionConfig());

        MyPOJO[] types = {
            new MyPOJO(72, new Date(763784523L), new Date(88234L)),
            new MyPOJO(-1, new Date(11111111111111L)),
            new MyPOJO(42),
            new MyPOJO(17, new Date(222763784523L))
        };

        for (MyPOJO val : types) {
            byte[] serialized = schema.serialize(val);
            MyPOJO deser = schema.deserialize(serialized);
            assertThat(deser).isEqualTo(val);
        }
    }

    @Test
    void testSerializability() throws IOException {
        TypeInformation<MyPOJO> info = TypeExtractor.getForClass(MyPOJO.class);
        TypeInformationSerializationSchema<MyPOJO> schema =
                new TypeInformationSerializationSchema<>(info, new ExecutionConfig());

        // this needs to succeed
        CommonTestUtils.createCopySerializable(schema);
    }

    // ------------------------------------------------------------------------
    //  Test data types
    // ------------------------------------------------------------------------

    private static class MyPOJO {

        public int aField;
        public List<Date> aList;

        public MyPOJO() {}

        public MyPOJO(int iVal, Date... dates) {
            this.aField = iVal;
            this.aList = new ArrayList<>(Arrays.asList(dates));
        }

        @Override
        public int hashCode() {
            return aField;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof MyPOJO) {
                MyPOJO that = (MyPOJO) obj;
                return this.aField == that.aField
                        && (this.aList == null
                                ? that.aList == null
                                : that.aList != null && this.aList.equals(that.aList));
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return "MyPOJO " + aField + " " + aList;
        }
    }
}
