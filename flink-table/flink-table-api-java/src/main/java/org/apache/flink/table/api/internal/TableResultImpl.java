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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Implementation for {@link TableResult}.
 */
@Internal
class TableResultImpl implements TableResult {
	public static final TableResult TABLE_RESULT_OK = TableResultImpl.builder()
			.resultKind(ResultKind.SUCCESS)
			.tableSchema(TableSchema.builder().field("result", DataTypes.STRING()).build())
			.data(Collections.singletonList(Row.of("OK")))
			.build();

	private final JobClient jobClient;
	private final TableSchema tableSchema;
	private final ResultKind resultKind;
	private final Iterator<Row> data;
	private final PrintStyle printStyle;

	private TableResultImpl(
			@Nullable JobClient jobClient,
			TableSchema tableSchema,
			ResultKind resultKind,
			Iterator<Row> data,
			PrintStyle printStyle) {
		this.jobClient = jobClient;
		this.tableSchema = Preconditions.checkNotNull(tableSchema, "tableSchema should not be null");
		this.resultKind = Preconditions.checkNotNull(resultKind, "resultKind should not be null");
		this.data = Preconditions.checkNotNull(data, "data should not be null");
		this.printStyle = Preconditions.checkNotNull(printStyle, "printStyle should not be null");
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
		if (printStyle instanceof TableauStyle) {
			int maxColumnWidth = ((TableauStyle) printStyle).getMaxColumnWidth();
			String nullColumn = ((TableauStyle) printStyle).getNullColumn();
			PrintUtils.printAsTableauForm(
					getTableSchema(), it, new PrintWriter(System.out), maxColumnWidth, nullColumn);
		} else if (printStyle instanceof RawContentStyle) {
			while (it.hasNext()) {
				System.out.println(String.join(",", PrintUtils.rowToString(it.next())));
			}
		} else {
			throw new TableException("Unsupported print style: " + printStyle);
		}
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
		private PrintStyle printStyle = PrintStyle.tableau(Integer.MAX_VALUE, PrintUtils.NULL_COLUMN);

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
		 * Specifies an row iterator as the execution result.
		 *
		 * @param rowIterator a row iterator as the execution result.
		 */
		public Builder data(Iterator<Row> rowIterator) {
			Preconditions.checkNotNull(rowIterator, "rowIterator should not be null");
			this.data = rowIterator;
			return this;
		}

		/**
		 * Specifies an row list as the execution result.
		 *
		 * @param rowList a row list as the execution result.
		 */
		public Builder data(List<Row> rowList) {
			Preconditions.checkNotNull(rowList, "listRows should not be null");
			this.data = rowList.iterator();
			return this;
		}

		/**
		 * Specifies print style. Default is {@link TableauStyle} with max integer column width.
		 */
		public Builder setPrintStyle(PrintStyle printStyle) {
			Preconditions.checkNotNull(printStyle, "printStyle should not be null");
			this.printStyle = printStyle;
			return this;
		}

		/**
		 * Returns a {@link TableResult} instance.
		 */
		public TableResult build() {
			return new TableResultImpl(jobClient, tableSchema, resultKind, data, printStyle);
		}
	}

	/**
	 * Root interface for all print styles.
	 */
	public interface PrintStyle {
		/**
		 * Create a tableau print style with given max column width and null column,
		 * which prints the result schema and content as tableau form.
		 */
		static PrintStyle tableau(int maxColumnWidth, String nullColumn) {
			Preconditions.checkArgument(maxColumnWidth > 0, "maxColumnWidth should be greater than 0");
			Preconditions.checkNotNull(nullColumn, "nullColumn should not be null");
			return new TableauStyle(maxColumnWidth, nullColumn);
		}

		/**
		 * Create a raw content print style,
		 * which only print the result content as raw form.
		 * column delimiter is ",", row delimiter is "\n".
		 */
		static PrintStyle rawContent() {
			return new RawContentStyle();
		}
	}

	/**
	 * print the result schema and content as tableau form.
	 */
	private static final class TableauStyle implements PrintStyle {

		private final int maxColumnWidth;
		private final String nullColumn;

		private TableauStyle(int maxColumnWidth, String nullColumn) {
			this.maxColumnWidth = maxColumnWidth;
			this.nullColumn = nullColumn;
		}

		int getMaxColumnWidth() {
			return maxColumnWidth;
		}

		String getNullColumn() {
			return nullColumn;
		}
	}

	/**
	 * only print the result content as raw form.
	 * column delimiter is ",", row delimiter is "\n".
	 */
	private static final class RawContentStyle implements PrintStyle {
	}

}
