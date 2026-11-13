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

package org.apache.flink.table.types.logical;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.binary.BinaryGeographyData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeographyTypeTest {

    @Test
    void testSupportsOnlyRegisteredBridgeClasses() {
        GeographyType type = new GeographyType();

        assertThat(type.supportsInputConversion(GeographyData.class)).isTrue();
        assertThat(type.supportsOutputConversion(GeographyData.class)).isTrue();
        assertThat(type.supportsInputConversion(BinaryGeographyData.class)).isTrue();
        assertThat(type.supportsOutputConversion(BinaryGeographyData.class)).isTrue();
        assertThat(type.supportsInputConversion(CustomGeographyData.class)).isFalse();
        assertThat(type.supportsOutputConversion(CustomGeographyData.class)).isFalse();
    }

    @Test
    void testBridgedToRejectsUnsupportedGeographyImplementation() {
        assertThatThrownBy(() -> DataTypes.GEOGRAPHY().bridgedTo(CustomGeographyData.class))
                .isInstanceOf(ValidationException.class);
    }

    private static class CustomGeographyData implements GeographyData {
        @Override
        public int subtypeId() {
            return GeographyData.POINT;
        }

        @Override
        public int sizeInBytes() {
            return 0;
        }

        @Override
        public byte[] toBytes() {
            return new byte[0];
        }
    }
}
