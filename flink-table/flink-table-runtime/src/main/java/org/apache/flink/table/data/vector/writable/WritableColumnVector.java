/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector.writable;

import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.Dictionary;

/** Writable {@link ColumnVector}. */
public interface WritableColumnVector extends ColumnVector {

    /** Resets the column to default state. */
    void reset();

    /** Set null at rowId. */
    void setNullAt(int rowId);

    /** Set nulls from rowId to rowId + count (exclude). */
    void setNulls(int rowId, int count);

    /** Fill the column vector with nulls. */
    void fillWithNulls();

    /** Set the dictionary, it should work with dictionary ids. */
    void setDictionary(Dictionary dictionary);

    /** Check if there's a dictionary. */
    boolean hasDictionary();

    /**
     * Reserve a integer column for ids of dictionary. The size of return {@link WritableIntVector}
     * should be equal to or bigger than capacity. DictionaryIds must inconsistent with {@link
     * #setDictionary}. We don't support a mix of dictionary.
     */
    WritableIntVector reserveDictionaryIds(int capacity);

    /** Get reserved dictionary ids. */
    WritableIntVector getDictionaryIds();
}
