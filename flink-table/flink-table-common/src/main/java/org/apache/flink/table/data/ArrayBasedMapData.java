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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.FlinkRuntimeException;

/** A simple `MapData` implementation which is backed by 2 arrays. */
@PublicEvolving
public class ArrayBasedMapData implements MapData {
    private final ArrayData keyArray;
    private final ArrayData valueArray;

    public ArrayBasedMapData(ArrayData keyArray, ArrayData valueArray) {
        if (keyArray.size() != valueArray.size()) {
            throw new FlinkRuntimeException(
                    "Invalid function call:\n"
                            + "The length of the keys array "
                            + keyArray.size()
                            + " is not equal to the length of the values array "
                            + valueArray.size());
        }
        this.keyArray = keyArray;
        this.valueArray = valueArray;
    }

    @Override
    public int size() {
        return keyArray.size();
    }

    @Override
    public ArrayData keyArray() {
        return keyArray;
    }

    @Override
    public ArrayData valueArray() {
        return valueArray;
    }
}
