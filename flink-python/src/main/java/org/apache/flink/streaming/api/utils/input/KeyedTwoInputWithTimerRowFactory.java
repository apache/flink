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

package org.apache.flink.streaming.api.utils.input;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.types.Row;

/**
 * This factory produces runner input row for two input operators which need to send the timer
 * trigger event to python side.
 */
public class KeyedTwoInputWithTimerRowFactory {

    /** Reusable row for normal data runner inputs. */
    private final Row reuseNormalData;

    private final KeyedInputWithTimerRowFactory oneInputFactory;

    public KeyedTwoInputWithTimerRowFactory() {
        this.reuseNormalData = new Row(3);
        this.oneInputFactory = new KeyedInputWithTimerRowFactory();
    }

    public Row fromNormalData(boolean isLeft, long timestamp, long watermark, Row userInput) {
        reuseNormalData.setField(0, isLeft);
        if (isLeft) {
            // The input row is a tuple of key and value.
            reuseNormalData.setField(1, userInput);
            // need to set null since it is a reuse row.
            reuseNormalData.setField(2, null);
        } else {
            // need to set null since it is a reuse row.
            reuseNormalData.setField(1, null);
            // The input row is a tuple of key and value.
            reuseNormalData.setField(2, userInput);
        }

        return oneInputFactory.fromNormalData(timestamp, watermark, reuseNormalData);
    }

    public Row fromTimer(
            TimeDomain timeDomain,
            long timestamp,
            long watermark,
            Row key,
            byte[] encodedNamespace) {
        return oneInputFactory.fromTimer(timeDomain, timestamp, watermark, key, encodedNamespace);
    }

    public static TypeInformation<Row> getRunnerInputTypeInfo(
            TypeInformation<Row> userInputType1,
            TypeInformation<Row> userInputType2,
            TypeInformation<Row> keyType) {
        // leftInput, rightInput structure:
        // [key, userInput]

        // structure: [isLeftUserInput, leftInput, rightInput]
        RowTypeInfo normalDataTypeInfo =
                new RowTypeInfo(Types.BOOLEAN, userInputType1, userInputType2);

        // structure: [timerType, key, serializedNamespace]
        RowTypeInfo timerDataTypeInfo =
                new RowTypeInfo(Types.BYTE, keyType, Types.PRIMITIVE_ARRAY(Types.BYTE));

        // structure: [runnerInputType, normalData, timestamp, watermark, timerData]
        return Types.ROW(Types.BYTE, normalDataTypeInfo, Types.LONG, Types.LONG, timerDataTypeInfo);
    }
}
