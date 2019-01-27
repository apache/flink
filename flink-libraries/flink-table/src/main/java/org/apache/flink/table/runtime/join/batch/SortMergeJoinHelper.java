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

package org.apache.flink.table.runtime.join.batch;

import org.apache.flink.table.codegen.JoinConditionFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.util.Collector;

import java.util.BitSet;

/**
 * Helper class for sort merge join operators. Contains some join methods.
 */
public class SortMergeJoinHelper {

	private final Collector<BaseRow> collector;
	private final JoinConditionFunction condFunc;

	private final BaseRow leftNullRow;
	private final BaseRow rightNullRow;
	private final JoinedRow joinedRow;

	public SortMergeJoinHelper(
		Collector<BaseRow> collector,
		JoinConditionFunction condFunc,
		BaseRow leftNullRow,
		BaseRow rightNullRow,
		JoinedRow joinedRow) {
		this.collector = collector;
		this.condFunc = condFunc;

		this.leftNullRow = leftNullRow;
		this.rightNullRow = rightNullRow;
		this.joinedRow = joinedRow;
	}

	public void innerJoin(
		SortMergeInnerJoinIterator iterator, boolean reverseInvoke) throws Exception {
		while (iterator.nextInnerJoin()) {
			BaseRow probeRow = iterator.getProbeRow();
			ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator();
			while (iter.advanceNext()) {
				BaseRow row = iter.getRow();
				joinWithCondition(probeRow, row, reverseInvoke);
			}
			iter.close();
		}
	}

	public void oneSideOuterJoin(
		SortMergeOneSideOuterJoinIterator iterator, boolean reverseInvoke,
		BaseRow buildNullRow) throws Exception {
		while (iterator.nextOuterJoin()) {
			BaseRow probeRow = iterator.getProbeRow();
			boolean found = false;

			if (iterator.getMatchKey() != null) {
				ResettableExternalBuffer.BufferIterator iter = iterator.getMatchBuffer().newIterator();
				while (iter.advanceNext()) {
					BaseRow row = iter.getRow();
					found |= joinWithCondition(probeRow, row, reverseInvoke);
				}
				iter.close();
			}

			if (!found) {
				collect(probeRow, buildNullRow, reverseInvoke);
			}
		}
	}

	public void fullOuterJoin(SortMergeFullOuterJoinIterator iterator) throws Exception {
		BitSet bitSet = new BitSet();

		while (iterator.nextOuterJoin()) {

			bitSet.clear();
			BinaryRow matchKey = iterator.getMatchKey();
			ResettableExternalBuffer buffer1 = iterator.getBuffer1();
			ResettableExternalBuffer buffer2 = iterator.getBuffer2();

			if (matchKey == null && buffer1.size() > 0) { // left outer join.
				ResettableExternalBuffer.BufferIterator iter = buffer1.newIterator();
				while (iter.advanceNext()) {
					BaseRow row1 = iter.getRow();
					collector.collect(joinedRow.replace(row1, rightNullRow));
				}
				iter.close();
			} else if (matchKey == null && buffer2.size() > 0) { // right outer join.
				ResettableExternalBuffer.BufferIterator iter = buffer2.newIterator();
				while (iter.advanceNext()) {
					BaseRow row2 = iter.getRow();
					collector.collect(joinedRow.replace(leftNullRow, row2));
				}
				iter.close();
			} else if (matchKey != null) { // match join.
				ResettableExternalBuffer.BufferIterator iter1 = buffer1.newIterator();
				while (iter1.advanceNext()) {
					BaseRow row1 = iter1.getRow();
					boolean found = false;
					int index = 0;
					ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator();
					while (iter2.advanceNext()) {
						BaseRow row2 = iter2.getRow();
						if (condFunc.apply(row1, row2)) {
							collector.collect(joinedRow.replace(row1, row2));
							found = true;
							bitSet.set(index);
						}
						index++;
					}
					iter2.close();
					if (!found) {
						collector.collect(joinedRow.replace(row1, rightNullRow));
					}
				}
				iter1.close();

				// row2 outer
				int index = 0;
				ResettableExternalBuffer.BufferIterator iter2 = buffer2.newIterator();
				while (iter2.advanceNext()) {
					BaseRow row2 = iter2.getRow();
					if (!bitSet.get(index)) {
						collector.collect(joinedRow.replace(leftNullRow, row2));
					}
					index++;
				}
				iter2.close();
			} else { // bug...
				throw new RuntimeException("There is a bug.");
			}
		}
	}

	private boolean joinWithCondition(
		BaseRow row1, BaseRow row2, boolean reverseInvoke) throws Exception {
		if (reverseInvoke) {
			if (condFunc.apply(row2, row1)) {
				collector.collect(joinedRow.replace(row2, row1));
				return true;
			}
		} else {
			if (condFunc.apply(row1, row2)) {
				collector.collect(joinedRow.replace(row1, row2));
				return true;
			}
		}
		return false;
	}

	private void collect(
		BaseRow row1, BaseRow row2, boolean reverseInvoke) {
		if (reverseInvoke) {
			collector.collect(joinedRow.replace(row2, row1));
		} else {
			collector.collect(joinedRow.replace(row1, row2));
		}
	}
}
