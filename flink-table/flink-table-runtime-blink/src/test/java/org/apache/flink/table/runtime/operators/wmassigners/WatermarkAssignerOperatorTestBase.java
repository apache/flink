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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/** Base class for watermark assigner operator test. */
public abstract class WatermarkAssignerOperatorTestBase {

    protected Tuple2<Long, Long> validateElement(
            Object element, long nextElementValue, long currentWatermark) {
        if (element instanceof StreamRecord) {
            @SuppressWarnings("unchecked")
            StreamRecord<RowData> record = (StreamRecord<RowData>) element;
            assertEquals(nextElementValue, record.getValue().getLong(0));
            return new Tuple2<>(nextElementValue + 1, currentWatermark);
        } else if (element instanceof Watermark) {
            long wt = ((Watermark) element).getTimestamp();
            assertTrue(wt > currentWatermark);
            return new Tuple2<>(nextElementValue, wt);
        } else {
            throw new IllegalArgumentException("unrecognized element: " + element);
        }
    }

    protected List<Watermark> extractWatermarks(Collection<Object> collection) {
        List<Watermark> watermarks = new ArrayList<>();
        for (Object obj : collection) {
            if (obj instanceof Watermark) {
                watermarks.add((Watermark) obj);
            }
        }
        return watermarks;
    }
}
