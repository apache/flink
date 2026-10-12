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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataViewUtils}. */
class DataViewUtilsTest {

    @Test
    void testValueViewInAccumulatorFieldIsRejected() {
        final DataType accumulatorDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("v", ValueView.newValueViewDataType(DataTypes.INT())));

        assertThatThrownBy(() -> DataViewUtils.adjustDataViews(accumulatorDataType, true))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "ValueView is not supported in accumulators of aggregating functions");
    }

    @Test
    void testValueViewAsWholeAccumulatorIsRejected() {
        final DataType accumulatorDataType = ValueView.newValueViewDataType(DataTypes.INT());

        assertThatThrownBy(() -> DataViewUtils.adjustDataViews(accumulatorDataType, false))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "ValueView is not supported in accumulators of aggregating functions");
    }

    @Test
    void testListAndMapViewsInAccumulatorAreAllowed() {
        final DataType accumulatorDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("l", ListView.newListViewDataType(DataTypes.INT())),
                        DataTypes.FIELD(
                                "m",
                                MapView.newMapViewDataType(DataTypes.STRING(), DataTypes.INT())));

        assertThatCode(() -> DataViewUtils.adjustDataViews(accumulatorDataType, true))
                .doesNotThrowAnyException();
    }
}
