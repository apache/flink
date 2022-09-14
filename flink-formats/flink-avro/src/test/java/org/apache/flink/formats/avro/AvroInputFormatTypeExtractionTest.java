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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the type extraction of the {@link AvroInputFormat}. */
class AvroInputFormatTypeExtractionTest {

    @Test
    void testTypeExtraction() {
        try {
            InputFormat<MyAvroType, ?> format =
                    new AvroInputFormat<MyAvroType>(
                            new Path("file:///ignore/this/file"), MyAvroType.class);

            TypeInformation<?> typeInfoDirect = TypeExtractor.getInputFormatTypes(format);

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSet<MyAvroType> input = env.createInput(format);
            TypeInformation<?> typeInfoDataSet = input.getType();

            assertThat(typeInfoDirect).isInstanceOf(PojoTypeInfo.class);
            assertThat(typeInfoDataSet).isInstanceOf(PojoTypeInfo.class);

            assertThat(typeInfoDirect.getTypeClass()).isEqualTo(MyAvroType.class);
            assertThat(typeInfoDataSet.getTypeClass()).isEqualTo(MyAvroType.class);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /** Test type. */
    public static final class MyAvroType {

        public String theString;

        public MyAvroType recursive;

        private double aDouble;

        public double getaDouble() {
            return aDouble;
        }

        public void setaDouble(double aDouble) {
            this.aDouble = aDouble;
        }

        public void setTheString(String theString) {
            this.theString = theString;
        }

        public String getTheString() {
            return theString;
        }
    }
}
