/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for stream operators that can select the input to get {@link
 * org.apache.flink.streaming.runtime.streamrecord.StreamRecord}.
 *
 * <p><b>IMPORTANT:</b> This interface is a loose contract. The runtime may read multiple records
 * continuously before calling {@code nextSelection()} again to determine whether to change the
 * input to be read. That is, it is not guaranteed that {@code nextSelection()} will be called
 * immediately after the operator has processed a record and the reading input will be changed
 * according to {@link InputSelection} returned. This means that the operator may receive some data
 * that it does not currently want to process. Therefore, if an operator needs a strict convention,
 * it must cache the unexpected data itself and handle them correctly.
 *
 * <p>This interface also makes the following conventions:
 *
 * <ol>
 *   <li>The runtime must call {@code nextSelection()} to determine the input to read the first
 *       record.
 *   <li>When the input being read reaches the end, the runtime must call {@code nextSelection()} to
 *       determine the next input to be read.
 * </ol>
 */
@PublicEvolving
public interface InputSelectable {

    /**
     * Returns the next {@link InputSelection} that wants to get the record. This method is
     * guaranteed to not be called concurrently with other methods of the operator.
     */
    InputSelection nextSelection();
}
