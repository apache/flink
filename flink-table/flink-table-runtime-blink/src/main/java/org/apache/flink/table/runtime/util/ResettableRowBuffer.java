/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.io.Closeable;
import java.io.IOException;

/**
 * Resettable buffer that add {@link RowData} and return {@link BinaryRowData} iterator.
 *
 * <p>Instructions: 1.{@link #reset()} 2.multi {@link #add(RowData)} 3.{@link #complete()} 4.multi
 * {@link #newIterator()} repeat the above steps or {@link #close()}.
 */
public interface ResettableRowBuffer extends Closeable {

    /** Re-initialize the buffer state. */
    void reset();

    /** Appends the specified row to the end of this buffer. */
    void add(RowData row) throws IOException;

    /** Finally, complete add. */
    void complete();

    /** Get a new iterator starting from first row. */
    ResettableIterator newIterator();

    /** Get a new iterator starting from the `beginRow`-th row. `beginRow` is 0-indexed. */
    ResettableIterator newIterator(int beginRow);

    /** Row iterator that can be reset. */
    interface ResettableIterator extends RowIterator<BinaryRowData>, Closeable {

        /** Re-initialize the iterator, start from begin row. */
        void reset() throws IOException;
    }
}
