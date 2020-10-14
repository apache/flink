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

package org.apache.flink.table.runtime.operators.window.join;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

/** Collectors used for patching up the window attributes of join LHS and RHS. **/
public class WindowAttrCollectors {
	private WindowAttrCollectors() {}

	public static WindowAttrCollector getWindowAttrCollector(WindowAttribute windowAttribute) {
		switch (windowAttribute) {
		case NONE:
			return new NoneWindowAttrCollector();
		case START:
			return new StartWindowAttrCollector();
		case END:
			return new EndWindowAttrCollector();
		case START_END:
			return new StartEndWindowAttrCollector();
		default:
			throw new IllegalArgumentException("Unexpected window attribute kind: "
					+ windowAttribute);
		}
	}

	/** The window attribute collector. */
	public interface WindowAttrCollector {
		RowData collect(RowData dataRow, TimeWindow window);
	}

	/** Patches up the "window_start" attribute. */
	public static class StartWindowAttrCollector implements WindowAttrCollector {
		private final GenericRowData reusedWindowAttrs = new GenericRowData(1);
		private final JoinedRowData reusedWrapperRow = new JoinedRowData();

		public RowData collect(
				RowData dataRow,
				TimeWindow window) {
			reusedWindowAttrs.setField(0, TimestampData.fromEpochMillis(window.getStart()));
			return reusedWrapperRow.replace(dataRow, reusedWindowAttrs);
		}
	}

	/** Patches up the "window_end" attribute. */
	public static class EndWindowAttrCollector implements WindowAttrCollector {
		private final GenericRowData reusedWindowAttrs = new GenericRowData(1);
		private final JoinedRowData reusedWrapperRow = new JoinedRowData();

		public RowData collect(
				RowData dataRow,
				TimeWindow window) {
			reusedWindowAttrs.setField(0, TimestampData.fromEpochMillis(window.getEnd()));
			return reusedWrapperRow.replace(dataRow, reusedWindowAttrs);
		}
	}

	/** Patches up the "window_start" and "window_end" attributes. */
	public static class StartEndWindowAttrCollector implements WindowAttrCollector {
		private final GenericRowData reusedWindowAttrs = new GenericRowData(2);
		private final JoinedRowData reusedWrapperRow = new JoinedRowData();

		public RowData collect(
				RowData dataRow,
				TimeWindow window) {
			reusedWindowAttrs.setField(0, TimestampData.fromEpochMillis(window.getStart()));
			reusedWindowAttrs.setField(1, TimestampData.fromEpochMillis(window.getEnd()));
			return reusedWrapperRow.replace(dataRow, reusedWindowAttrs);
		}
	}

	/** Does not patch up any window attributes. */
	public static class NoneWindowAttrCollector implements WindowAttrCollector {

		public RowData collect(
				RowData dataRow,
				TimeWindow window) {
			return dataRow;
		}
	}
}
