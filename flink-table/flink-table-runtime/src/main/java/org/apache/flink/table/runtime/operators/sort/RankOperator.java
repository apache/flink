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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

/** Rank operator to compute top N. */
public class RankOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private GeneratedRecordComparator partitionByGenComp;
    private GeneratedRecordComparator orderByGenComp;
    private final long rankStart;
    private final long rankEnd;
    private final boolean outputRankFunColumn;

    private transient RecordComparator partitionByComp;
    private transient RecordComparator orderByComp;
    private transient long rowNum;
    private transient long rank;
    private transient GenericRowData rankValueRow;
    private transient JoinedRowData joinedRow;
    private transient RowData lastInput;
    private transient StreamRecordCollector<RowData> collector;
    private transient AbstractRowDataSerializer<RowData> inputSer;

    public RankOperator(
            GeneratedRecordComparator partitionByGenComp,
            GeneratedRecordComparator orderByGenComp,
            long rankStart,
            long rankEnd,
            boolean outputRankFunColumn) {
        this.partitionByGenComp = partitionByGenComp;
        this.orderByGenComp = orderByGenComp;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankFunColumn = outputRankFunColumn;
    }

    @Override
    public void open() throws Exception {
        super.open();

        ClassLoader cl = getUserCodeClassloader();
        inputSer = (AbstractRowDataSerializer) getOperatorConfig().getTypeSerializerIn1(cl);

        partitionByComp = partitionByGenComp.newInstance(cl);
        partitionByGenComp = null;

        orderByComp = orderByGenComp.newInstance(cl);
        orderByGenComp = null;

        if (outputRankFunColumn) {
            joinedRow = new JoinedRowData();
            rankValueRow = new GenericRowData(1);
        }

        collector = new StreamRecordCollector<>(output);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        // add 1 when meets a new row
        rowNum += 1L;
        if (lastInput == null || partitionByComp.compare(lastInput, input) != 0) {
            // reset rank value and row number value for new group
            rank = 1L;
            rowNum = 1L;
        } else if (orderByComp.compare(lastInput, input) != 0) {
            // set rank value as row number value if order-by value is change in a group
            rank = rowNum;
        }

        emitInternal(input);
        lastInput = inputSer.copy(input);
    }

    private void emitInternal(RowData element) {
        if (rank >= rankStart && rank <= rankEnd) {
            if (outputRankFunColumn) {
                rankValueRow.setField(0, rank);
                collector.collect(joinedRow.replace(element, rankValueRow));
            } else {
                collector.collect(element);
            }
        }
    }
}
