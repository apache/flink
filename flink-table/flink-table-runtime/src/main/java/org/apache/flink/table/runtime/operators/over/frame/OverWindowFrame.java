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

package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;

import java.io.Serializable;

/**
 * A window frame calculates the results for those records belong to a window frame. Before use a
 * frame must be prepared by passing it all the records in the current partition. A frame is a
 * subset of the current partition and the frame clause specifies how to define the subset. Frames
 * are determined with respect to the current row, which enables a frame to move within a partition
 * depending on the location of the current row within its partition. More information:
 * https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/window-functions-frames.html
 *
 * <p>E.g.: SELECT d, e, f, sum(e) over (partition by d order by e rows between 5 PRECEDING and 2
 * FOLLOWING), -- frame 1 count(*) over (partition by d order by e desc rows between 6 PRECEDING and
 * 2 FOLLOWING), -- frame 2 max(f) over (partition by d order by e rows between UNBOUNDED PRECEDING
 * and CURRENT ROW), -- frame 3 min(h) over (partition by d order by e desc rows between CURRENT ROW
 * and UNBOUNDED FOLLOWING), -- frame 4 h FROM Table5 The above sql has 4 frames.
 *
 * <p>Over AGG means that every Row has a corresponding output. OverWindowFrame is called by: 1.Get
 * all data and invoke {@link #prepare(ResettableExternalBuffer)} for partition 2.Then each Row is
 * traversed one by one to invoke {@link #process(int, RowData)} to get the calculation results of
 * the currentRow.
 */
public interface OverWindowFrame extends Serializable {

    /** Open to init with {@link ExecutionContext}. */
    void open(ExecutionContext ctx) throws Exception;

    /** Prepare for next partition. */
    void prepare(ResettableExternalBuffer rows) throws Exception;

    /** return the ACC of the window frame. */
    RowData process(int index, RowData current) throws Exception;

    /**
     * Get next row from iterator. Return null if iterator has no next. TODO Maybe copy is repeated.
     */
    static BinaryRowData getNextOrNull(ResettableExternalBuffer.BufferIterator iterator) {
        return iterator.advanceNext() ? iterator.getRow().copy() : null;
    }
}
