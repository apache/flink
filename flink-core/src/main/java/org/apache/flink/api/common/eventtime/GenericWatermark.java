/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

/**
 * Watermarks are the progress indicators in the data streams. A watermark signifies that no events
 * with a timestamp smaller or equal to the watermark's time will occur after the water. A watermark
 * with timestamp <i>T</i> indicates that the stream's event time has progressed to time <i>T</i>.
 *
 * <p>Watermarks are created at the sources and propagate through the streams and operators.
 *
 * <p>In some cases a watermark is only a heuristic, meaning some events with a lower timestamp may
 * still follow. In that case, it is up to the logic of the operators to decide what to do with the
 * "late events". Operators can for example ignore these late events, route them to a different
 * stream, or send update to their previously emitted results.
 *
 * <p>When a source reaches the end of the input, it emits a final watermark with timestamp {@code
 * Long.MAX_VALUE}, indicating the "end of time".
 *
 * <p>Note: A stream's time starts with a watermark of {@code Long.MIN_VALUE}. That means that all
 * records in the stream with a timestamp of {@code Long.MIN_VALUE} are immediately late.
 */
@Public
public interface GenericWatermark extends Serializable {


    /**
     * Compares this watermark with another watermark.
     *
     * @param laterWatermark The watermark to compare with. Can be null.
     * @return An Integer containing the comparison result:
     *         - A positive integer if this watermark is later.
     *         - A negative integer if this watermark is earlier.
     *         - Zero if both are equal.
     */
//    int checkUpdated(GenericWatermark laterWatermark);
//
//    boolean isComparable();
//    /**
//     * Creates a copy of this watermark.
//     *
//     * @return A new instance that is a copy of this watermark.
//     */
//    GenericWatermark copy();
//
//    void serialize(DataOutputView target) throws IOException;
//
//    GenericWatermark deserialize(DataInputView inputView) throws IOException;
}
