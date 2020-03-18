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

package org.apache.flink.table.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;
import org.jline.terminal.Terminal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.table.client.cli.CliUtils.getStringDisplayWidth;
import static org.apache.flink.table.client.cli.CliUtils.isFullWidth;
import static org.apache.flink.table.client.cli.CliUtils.rowToString;

/**
 * Print result in tableau mode.
 */
public class CliTableauResultView implements AutoCloseable {

	private static final int NULL_COLUMN_WIDTH = CliStrings.NULL_COLUMN.length();
	private static final int MAX_COLUMN_WIDTH = 30;
	private static final int DEFAULT_COLUMN_WIDTH = 20;
	private static final String COLUMN_TRUNCATED_FLAG = "...";
	private static final String CHANGEFLAG_COLUMN_NAME = "+/-";

	private final Terminal terminal;
	private final Executor sqlExecutor;
	private final String sessionId;
	private final ResultDescriptor resultDescriptor;
	private final ExecutorService displayResultExecutorService;

	public CliTableauResultView(
			final Terminal terminal,
			final Executor sqlExecutor,
			final String sessionId,
			final ResultDescriptor resultDescriptor) {
		this.terminal = terminal;
		this.sqlExecutor = sqlExecutor;
		this.sessionId = sessionId;
		this.resultDescriptor = resultDescriptor;
		this.displayResultExecutorService = Executors.newSingleThreadExecutor();
	}

	public void displayStreamResults() throws SqlExecutionException {
		final AtomicInteger receivedRowCount = new AtomicInteger(0);
		Future<?> resultFuture = displayResultExecutorService.submit(() -> {
			printStreamResults(receivedRowCount);
		});

		// capture CTRL-C
		terminal.handle(Terminal.Signal.INT, signal -> {
			resultFuture.cancel(true);
		});

		boolean cleanUpQuery = true;
		try {
			resultFuture.get();
			cleanUpQuery = false; // job finished successfully
		} catch (CancellationException e) {
			terminal.writer().println("Query terminated, received a total of " + receivedRowCount.get() + " rows");
			terminal.flush();
		} catch (ExecutionException e) {
			if (e.getCause() instanceof SqlExecutionException) {
				throw (SqlExecutionException) e.getCause();
			}
			throw new SqlExecutionException("unknown exception", e.getCause());
		} catch (InterruptedException e) {
			throw new SqlExecutionException("Query interrupted", e);
		} finally {
			checkAndCleanUpQuery(cleanUpQuery);
		}
	}

	public void displayBatchResults() throws SqlExecutionException {
		Future<?> resultFuture = displayResultExecutorService.submit(() -> {
			final List<Row> resultRows = waitBatchResults();
			printBatchResults(resultRows);
		});

		// capture CTRL-C
		terminal.handle(Terminal.Signal.INT, signal -> {
			resultFuture.cancel(true);
		});

		boolean cleanUpQuery = true;
		try {
			resultFuture.get();
			cleanUpQuery = false; // job finished successfully
		} catch (CancellationException e) {
			terminal.writer().println("Query terminated");
			terminal.flush();
		} catch (ExecutionException e) {
			if (e.getCause() instanceof SqlExecutionException) {
				throw (SqlExecutionException) e.getCause();
			}
			throw new SqlExecutionException("unknown exception", e.getCause());
		} catch (InterruptedException e) {
			throw new SqlExecutionException("Query interrupted", e);
		} finally {
			checkAndCleanUpQuery(cleanUpQuery);
		}
	}

	@Override
	public void close() {
		this.displayResultExecutorService.shutdown();
	}

	private void checkAndCleanUpQuery(boolean cleanUpQuery) {
		if (cleanUpQuery) {
			try {
				sqlExecutor.cancelQuery(sessionId, resultDescriptor.getResultId());
			} catch (SqlExecutionException e) {
				// ignore further exceptions
			}
		}
	}

	private List<Row> waitBatchResults() {
		List<Row> resultRows;
		// take snapshot and make all results in one page
		do {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			TypedResult<Integer> result = sqlExecutor.snapshotResult(
					sessionId,
					resultDescriptor.getResultId(),
					Integer.MAX_VALUE);

			if (result.getType() == TypedResult.ResultType.EOS) {
				resultRows = Collections.emptyList();
				break;
			} else if (result.getType() == TypedResult.ResultType.PAYLOAD) {
				resultRows = sqlExecutor.retrieveResultPage(resultDescriptor.getResultId(), 1);
				break;
			} else {
				// result not retrieved yet
			}
		} while (true);

		return resultRows;
	}

	private void printStreamResults(AtomicInteger receivedRowCount) {
		List<TableColumn> columns = resultDescriptor.getResultSchema().getTableColumns();
		String[] fieldNames =
				Stream.concat(
						Stream.of("+/-"),
						columns.stream().map(TableColumn::getName)
				).toArray(String[]::new);

		int[] colWidths = columnWidthsByType(columns, true);
		String borderline = genBorderLine(colWidths);

		// print filed names
		terminal.writer().println(borderline);
		printSingleRow(colWidths, fieldNames);
		terminal.writer().println(borderline);
		terminal.flush();

		while (true) {
			final TypedResult<List<Tuple2<Boolean, Row>>> result =
					sqlExecutor.retrieveResultChanges(sessionId, resultDescriptor.getResultId());

			switch (result.getType()) {
				case EMPTY:
					// do nothing
					break;
				case EOS:
					if (receivedRowCount.get() > 0) {
						terminal.writer().println(borderline);
					}
					terminal.writer().println("Received a total of " + receivedRowCount.get() + " rows");
					terminal.flush();
					return;
				case PAYLOAD:
					List<Tuple2<Boolean, Row>> changes = result.getPayload();
					for (Tuple2<Boolean, Row> change : changes) {
						final String[] cols = rowToString(change.f1);
						String[] row = new String[cols.length + 1];
						row[0] = change.f0 ? "+" : "-";
						System.arraycopy(cols, 0, row, 1, cols.length);
						printSingleRow(colWidths, row);
						receivedRowCount.incrementAndGet();
					}
					break;
				default:
					throw new SqlExecutionException("Unknown result type: " + result.getType());
			}
		}
	}

	private void printBatchResults(List<Row> resultRows) {
		List<String[]> rows = new ArrayList<>(resultRows.size() + 1);

		// fill field names first
		List<TableColumn> columns = resultDescriptor.getResultSchema().getTableColumns();
		rows.add(columns.stream().map(TableColumn::getName).toArray(String[]::new));
		resultRows.forEach(row -> rows.add(rowToString(row)));

		int[] colWidths = columnWidthsByContent(columns, rows);
		String borderline = genBorderLine(colWidths);

		// print field names
		terminal.writer().println(borderline);
		printSingleRow(colWidths, rows.get(0));
		terminal.writer().println(borderline);

		// print content
		rows.subList(1, rows.size()).forEach(row -> printSingleRow(colWidths, row));
		if (!resultRows.isEmpty()) {
			terminal.writer().println(borderline);
		}

		// print footer
		terminal.writer().println(resultRows.size() + " row in set");
		terminal.flush();
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
				sb.append(truncateString(col, colWidths[idx] - COLUMN_TRUNCATED_FLAG.length()));
				sb.append(COLUMN_TRUNCATED_FLAG);
			}
			sb.append(" |");
			idx++;
		}
		terminal.writer().println(sb.toString());
		terminal.flush();
	}

	/**
	 * Try to infer column width based on column types. In streaming case, we will have an
	 * endless result set, thus couldn't determine column widths based on column values.
	 */
	private int[] columnWidthsByType(List<TableColumn> columns, boolean includeChangeflag) {
		// fill width with field names first
		int[] colWidths = columns.stream()
				.mapToInt(col -> col.getName().length())
				.toArray();

		// determine proper column width based on types
		for (int i = 0; i < columns.size(); ++i) {
			LogicalType type = columns.get(i).getType().getLogicalType();
			int len;
			switch (type.getTypeRoot()) {
				case TINYINT:
					len = TinyIntType.PRECISION + 1; // extra for negative value
					break;
				case SMALLINT:
					len = SmallIntType.PRECISION + 1; // extra for negative value
					break;
				case INTEGER:
					len = IntType.PRECISION + 1; // extra for negative value
					break;
				case BIGINT:
					len = BigIntType.PRECISION + 1; // extra for negative value
					break;
				case DECIMAL:
					len = ((DecimalType) type).getPrecision() + 2; // extra for negative value and decimal point
					break;
				case BOOLEAN:
					len = 5; // "true" or "false"
					break;
				case DATE:
					len = 10; // e.g. 9999-12-31
					break;
				case TIME_WITHOUT_TIME_ZONE:
					int precision = ((TimeType) type).getPrecision();
					len = precision == 0 ? 8 : precision + 9; // 23:59:59[.999999999]
					break;
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					precision = ((TimestampType) type).getPrecision();
					len = timestampTypeColumnWidth(precision);
					break;
				case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
					precision = ((LocalZonedTimestampType) type).getPrecision();
					len = timestampTypeColumnWidth(precision);
					break;
				default:
					len = DEFAULT_COLUMN_WIDTH;
			}

			// adjust column width with potential null values
			colWidths[i] = Math.max(colWidths[i], Math.max(len, NULL_COLUMN_WIDTH));
		}

		// add an extra column for change flag if necessary
		if (includeChangeflag) {
			int[] ret = new int[columns.size() + 1];
			ret[0] = CHANGEFLAG_COLUMN_NAME.length();
			System.arraycopy(colWidths, 0, ret, 1, columns.size());
			return ret;
		} else {
			return colWidths;
		}
	}

	/**
	 * Here we consider two popular class for timestamp: LocalDateTime and java.sql.Timestamp.
	 *
	 * <p>According to LocalDateTime's comment, the string output will be one of the following
	 * ISO-8601 formats:
	 *  <li>{@code uuuu-MM-dd'T'HH:mm:ss}</li>
	 *  <li>{@code uuuu-MM-dd'T'HH:mm:ss.SSS}</li>
	 *  <li>{@code uuuu-MM-dd'T'HH:mm:ss.SSSSSS}</li>
	 *  <li>{@code uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS}</li>
	 *
	 * <p>And for java.sql.Timestamp, the number of digits after point will be precision except
	 * when precision is 0. In that case, the format would be 'uuuu-MM-dd HH:mm:ss.0'
	 */
	int timestampTypeColumnWidth(int precision) {
		int base = 19; // length of uuuu-MM-dd HH:mm:ss
		if (precision == 0) {
			return base + 2; // consider java.sql.Timestamp
		} else if (precision <= 3) {
			return base + 4;
		} else if (precision <= 6) {
			return base + 7;
		} else {
			return base + 10;
		}
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

}
