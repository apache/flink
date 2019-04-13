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

package org.apache.flink.table.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.type.GenericType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.BinaryStringSerializer;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.typeutils.ListViewSerializer;
import org.apache.flink.table.typeutils.ListViewTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Iterator;
import java.util.List;

/**
 * built-in concat with retraction aggregate function.
 */
public class ConcatWithRetractAggFunction extends AggregateFunction<BinaryString, GenericRow> {

	private static final BinaryString lineDelimiter = BinaryString.fromString("\n");
	private ListViewSerializer<BinaryString> listViewSerializer =
			new ListViewSerializer<>(new ListSerializer<>(BinaryStringSerializer.INSTANCE));

	@Override
	public GenericRow createAccumulator() {
		// The accumulator schema:
		// list: BinaryGeneric<ListView<BinaryString>>
		// retractList: BinaryGeneric<ListView<BinaryString>>
		GenericRow acc = new GenericRow(2);
		// list
		acc.setField(0, new BinaryGeneric<>(new ListView<>(BinaryStringTypeInfo.INSTANCE), listViewSerializer));
		// retract list
		acc.setField(1, new BinaryGeneric<>(new ListView<>(BinaryStringTypeInfo.INSTANCE), listViewSerializer));
		return acc;
	}

	public void accumulate(GenericRow acc, BinaryString value) throws Exception {
		// ignore null value
		if (value != null) {
			ListView<BinaryString> list = getListViewFromAcc(acc, 0);
			list.add(value);
		}
	}

	public void merge(GenericRow acc, Iterable<GenericRow> its) throws Exception {
		Iterator<GenericRow> iter = its.iterator();
		while (iter.hasNext()) {
			GenericRow otherAcc = iter.next();
			ListView<BinaryString> thisList = getListViewFromAcc(acc, 0);
			ListView<BinaryString> otherList = getListViewFromAcc(otherAcc, 0);
			Iterable<BinaryString> accList = otherList.get();
			if (accList != null) {
				Iterator<BinaryString> listIter = accList.iterator();
				while (listIter.hasNext()) {
					thisList.add(listIter.next());
				}
			}

			ListView<BinaryString> otherRetractList = getListViewFromAcc(otherAcc, 1);
			ListView<BinaryString> thisRetractList = getListViewFromAcc(acc, 1);
			Iterable<BinaryString> retractList = otherRetractList.get();
			if (retractList != null) {
				Iterator<BinaryString> retractListIter = retractList.iterator();
				List<BinaryString> buffer = null;
				if (retractListIter.hasNext()) {
					buffer = (List<BinaryString>) thisList.get();
				}
				boolean listChanged = false;
				while (retractListIter.hasNext()) {
					BinaryString element = retractListIter.next();
					if (buffer != null && buffer.remove(element)) {
						listChanged = true;
					} else {
						thisRetractList.add(element);
					}
				}
				if (listChanged) {
					thisList.clear();
					thisList.addAll(buffer);
				}
			}
		}
	}

	public void retract(GenericRow acc, BinaryString value) throws Exception {
		if (value != null) {
			ListView<BinaryString> list = getListViewFromAcc(acc, 0);
			if (!list.remove(value)) {
				ListView<BinaryString> retractList = getListViewFromAcc(acc, 1);
				retractList.add(value);
			}
		}
	}

	@Override
	public BinaryString getValue(GenericRow acc) {
		ListView<BinaryString> list = getListViewFromAcc(acc, 0);
		try {
			Iterable<BinaryString> accList = list.get();
			if (accList == null || !accList.iterator().hasNext()) {
				// return null when the list is empty
				return null;
			} else {
				return BinaryString.concatWs(lineDelimiter, accList);
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}

	public void resetAccumulator(GenericRow acc) {
		ListView<BinaryString> list = getListViewFromAcc(acc, 0);
		ListView<BinaryString> retractList = getListViewFromAcc(acc, 1);
		list.clear();
		retractList.clear();
	}

	@Override
	public TypeInformation<GenericRow> getAccumulatorType() {
		InternalType[] fieldTypes = new InternalType[] {
				// it will be replaced to ListViewType
				new GenericType<>(new ListViewTypeInfo<>(BinaryStringTypeInfo.INSTANCE, false)),
				// it will be replaced to ListViewType
				new GenericType<>(new ListViewTypeInfo<>(BinaryStringTypeInfo.INSTANCE, false))
		};

		String[] fieldNames = new String[] {
				"list",
				"retractList"
		};

		return (TypeInformation) new BaseRowTypeInfo(fieldTypes, fieldNames);
	}

	private ListView<BinaryString> getListViewFromAcc(GenericRow acc, int ordinal) {
		BinaryGeneric<ListView<BinaryString>> binaryGeneric =
				(BinaryGeneric<ListView<BinaryString>>) acc.getField(ordinal);
		return BinaryGeneric.getJavaObjectFromBinaryGeneric(binaryGeneric, listViewSerializer);
	}
}
