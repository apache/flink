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

package org.apache.flink.runtime.state.heap.space;

import static org.apache.flink.runtime.state.heap.space.Constants.FOUR_BYTES_BITS;
import static org.apache.flink.runtime.state.heap.space.Constants.FOUR_BYTES_MARK;

/** Utils. */
public class SpaceUtils {

    public static int getChunkIdByAddress(long offset) {
        return (int) ((offset >>> FOUR_BYTES_BITS) & FOUR_BYTES_MARK);
    }

    public static int getChunkOffsetByAddress(long offset) {
        return (int) (offset & FOUR_BYTES_MARK);
    }
}
