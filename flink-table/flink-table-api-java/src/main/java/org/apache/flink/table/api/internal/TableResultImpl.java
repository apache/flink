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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Implementation for {@link TableResult}.
 */
@Internal
public class TableResultImpl implements TableResult {
	public static final TableResult TABLE_RESULT_OK = TableResultImpl.builder()
			.resultKind(ResultKind.SUCCESS)
			.tableSchema(TableSchema.builder().field("result", DataTypes.STRING()).build())
			.data(Collections.singletonList(Row.of("OK")))
			.build();

	// constants for printing
	private static final String NULL_COLUMN = "(NULL)";
	private static final int MAX_COLUMN_WIDTH = 30;
	private static final String COLUMN_TRUNCATED_FLAG = "...";

	private final JobClient jobClient;
	private final TableSchema tableSchema;
	private final ResultKind resultKind;
	private final Iterator<Row> data;

	private TableResultImpl(
			@Nullable JobClient jobClient,
			TableSchema tableSchema,
			ResultKind resultKind,
			Iterator<Row> data) {
		this.jobClient = jobClient;
		this.tableSchema = Preconditions.checkNotNull(tableSchema, "tableSchema should not be null");
		this.resultKind = Preconditions.checkNotNull(resultKind, "resultKind should not be null");
		this.data = Preconditions.checkNotNull(data, "data should not be null");
	}

	@Override
	public Optional<JobClient> getJobClient() {
		return Optional.ofNullable(jobClient);
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public ResultKind getResultKind() {
		return resultKind;
	}

	@Override
	public Iterator<Row> collect() {
		return data;
	}

	@Override
	public void print() {
		Iterator<Row> it = collect();
		doPrint(it);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for creating a {@link TableResultImpl}.
	 */
	public static class Builder {
		private JobClient jobClient = null;
		private TableSchema tableSchema = null;
		private ResultKind resultKind = null;
		private Iterator<Row> data = null;

		private Builder() {
		}

		/**
		 * Specifies job client which associates the submitted Flink job.
		 *
		 * @param jobClient a {@link JobClient} for the submitted Flink job.
		 */
		public Builder jobClient(JobClient jobClient) {
			this.jobClient = jobClient;
			return this;
		}

		/**
		 * Specifies table schema of the execution result.
		 *
		 * @param tableSchema a {@link TableSchema} for the execution result.
		 */
		public Builder tableSchema(TableSchema tableSchema) {
			Preconditions.checkNotNull(tableSchema, "tableSchema should not be null");
			this.tableSchema = tableSchema;
			return this;
		}

		/**
		 * Specifies result kind of the execution result.
		 *
		 * @param resultKind a {@link ResultKind} for the execution result.
		 */
		public Builder resultKind(ResultKind resultKind) {
			Preconditions.checkNotNull(resultKind, "resultKind should not be null");
			this.resultKind = resultKind;
			return this;
		}

		/**
		 * Specifies an row iterator as the execution result .
		 *
		 * @param rowIterator a row iterator as the execution result.
		 */
		public Builder data(Iterator<Row> rowIterator) {
			Preconditions.checkNotNull(rowIterator, "rowIterator should not be null");
			this.data = rowIterator;
			return this;
		}

		/**
		 * Specifies an row list as the execution result .
		 *
		 * @param rowList a row list as the execution result.
		 */
		public Builder data(List<Row> rowList) {
			Preconditions.checkNotNull(rowList, "listRows should not be null");
			this.data = rowList.iterator();
			return this;
		}

		/**
		 * Returns a {@link TableResult} instance.
		 */
		public TableResult build() {
			return new TableResultImpl(jobClient, tableSchema, resultKind, data);
		}
	}

	/**
	 * Displays the result in a tableau form.
	 * For example:
	 * +-------------+---------+-------------+
	 * | boolean_col | int_col | varchar_col |
	 * +-------------+---------+-------------+
	 * |        true |       1 |         abc |
	 * |       false |       2 |         def |
	 * |      (NULL) |  (NULL) |      (NULL) |
	 * +-------------+---------+-------------+
	 * 3 row(s) in result
	 *
	 * <p>Changelog is not supported until FLINK-16998 is finished.
	 */
	private void doPrint(Iterator<Row> it) {
		List<String[]> rows = new ArrayList<>();

		// fill field names first
		List<TableColumn> columns = getTableSchema().getTableColumns();
		rows.add(columns.stream().map(TableColumn::getName).toArray(String[]::new));
		while (it.hasNext()) {
			rows.add(rowToString(it.next()));
		}

		int[] colWidths = columnWidthsByContent(columns, rows);
		String borderline = genBorderLine(colWidths);

		// print field names
		System.out.println(borderline);
		printSingleRow(colWidths, rows.get(0));
		System.out.println(borderline);

		// print content
		if (rows.size() > 1) {
			rows.subList(1, rows.size()).forEach(row -> printSingleRow(colWidths, row));
			System.out.println(borderline);
		}

		System.out.println((rows.size() - 1) + " row(s) in result");
	}

	private String[] rowToString(Row row) {
		final String[] fields = new String[row.getArity()];
		for (int i = 0; i < row.getArity(); i++) {
			final Object field = row.getField(i);
			if (field == null) {
				fields[i] = NULL_COLUMN;
			} else {
				fields[i] = EncodingUtils.objectToString(field);
			}
		}
		return fields;
	}

	private int[] columnWidthsByContent(List<TableColumn> columns, List<String[]> rows) {
		// fill width with field names first
		int[] colWidths  = columns.stream().mapToInt(col -> col.getName().length()).toArray();

		// fill column width with real data
		for (String[] row : rows) {
			for (int i = 0; i < row.length; ++i) {
				colWidths[i] = Math.max(colWidths[i], getStringDisplayWidth(row[i]));
			}
		}

		// adjust column width with maximum length
		for (int i = 0; i < colWidths.length; ++i) {
			colWidths[i] = Math.min(colWidths[i], MAX_COLUMN_WIDTH);
		}

		return colWidths;
	}

	private String genBorderLine(int[] colWidths) {
		StringBuilder sb = new StringBuilder();
		sb.append("+");
		for (int width : colWidths) {
			sb.append(StringUtils.repeat('-', width + 1));
			sb.append("-+");
		}
		return sb.toString();
	}

	private void printSingleRow(int[] colWidths, String[] cols) {
		StringBuilder sb = new StringBuilder();
		sb.append("|");
		int idx = 0;
		for (String col : cols) {
			sb.append(" ");
			int displayWidth = getStringDisplayWidth(col);
			if (displayWidth <= colWidths[idx]) {
				sb.append(StringUtils.repeat(' ', colWidths[idx] - displayWidth));
				sb.append(col);
			} else {
				sb.append(truncateString(col, colWidths[idx] - COLUMN_TRUNCATED_FLAG.length())); //
				sb.append(COLUMN_TRUNCATED_FLAG);
			}
			sb.append(" |");
			idx++;
		}
		System.out.println(sb.toString());
	}

	private String truncateString(String col, int targetWidth) {
		int passedWidth = 0;
		int i = 0;
		for (; i < col.length(); i++) {
			if (isFullWidth(Character.codePointAt(col, i))) {
				passedWidth += 2;
			} else {
				passedWidth += 1;
			}
			if (passedWidth >= targetWidth) {
				break;
			}
		}
		String substring = col.substring(0, i);

		// pad with ' ' before the column
		int lackedWidth = targetWidth - getStringDisplayWidth(substring);
		if (lackedWidth > 0){
			substring = StringUtils.repeat(' ', lackedWidth) + substring;
		}
		return substring;
	}

	private int getStringDisplayWidth(String str) {
		int numOfFullWidthCh = (int) str.codePoints().filter(this::isFullWidth).count();
		return str.length() + numOfFullWidthCh;
	}

	/**
	 * Check codePoint is FullWidth or not according to Unicode Standard version 12.0.0.
	 * See http://unicode.org/reports/tr11/
	 */
	private boolean isFullWidth(int codePoint) {
		int value = UCharacter.getIntPropertyValue(codePoint, UProperty.EAST_ASIAN_WIDTH);
		switch (value) {
			case UCharacter.EastAsianWidth.NEUTRAL:
				return false;
			case UCharacter.EastAsianWidth.AMBIGUOUS:
				return false;
			case UCharacter.EastAsianWidth.HALFWIDTH:
				return false;
			case UCharacter.EastAsianWidth.FULLWIDTH:
				return true;
			case UCharacter.EastAsianWidth.NARROW:
				return false;
			case UCharacter.EastAsianWidth.WIDE:
				return true;
			default:
				throw new RuntimeException("unknown UProperty.EAST_ASIAN_WIDTH: " + value);
		}
	}
}
