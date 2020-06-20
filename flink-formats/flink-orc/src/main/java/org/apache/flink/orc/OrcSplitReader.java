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

package org.apache.flink.orc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.OrcVectorizedBatchWrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Orc split reader to read record from orc file. The reader is only responsible for reading the data
 * of a single split.
 */
public abstract class OrcSplitReader<T, BATCH> implements Closeable {

	private final OrcShim<BATCH> shim;

	// the ORC reader
	private RecordReader orcRowsReader;

	// the vectorized row data to be read in a batch
	protected final OrcVectorizedBatchWrapper<BATCH> rowBatchWrapper;

	// the number of rows in the current batch
	private int rowsInBatch;
	// the index of the next row to return
	protected int nextRow;

	public OrcSplitReader(
			OrcShim<BATCH> shim,
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			List<Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		this.shim = shim;
		this.orcRowsReader = shim.createRecordReader(
				conf, schema, selectedFields, conjunctPredicates, path, splitStart, splitLength);

		// create row batch
		this.rowBatchWrapper = shim.createBatchWrapper(schema, batchSize);
		rowsInBatch = 0;
		nextRow = 0;
	}

	/**
	 * Seek to a particular row number.
	 */
	public void seekToRow(long rowCount) throws IOException {
		orcRowsReader.seekToRow(rowCount);
	}

	@VisibleForTesting
	public RecordReader getRecordReader() {
		return orcRowsReader;
	}

	/**
	 * Method used to check if the end of the input is reached.
	 *
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public boolean reachedEnd() throws IOException {
		return !ensureBatch();
	}

	/**
	 * Fills an ORC batch into an array of Row.
	 *
	 * @return The number of rows that were filled.
	 */
	protected abstract int fillRows();

	/**
	 * Reads the next record from the input.
	 *
	 * @param reuse Object that may be reused.
	 * @return Read record.
	 *
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public abstract T nextRecord(T reuse) throws IOException;

	/**
	 * Checks if there is at least one row left in the batch to return.
	 * If no more row are available, it reads another batch of rows.
	 *
	 * @return Returns true if there is one more row to return, false otherwise.
	 * @throws IOException throw if an exception happens while reading a batch.
	 */
	private boolean ensureBatch() throws IOException {

		if (nextRow >= rowsInBatch) {
			// No more rows available in the Rows array.
			nextRow = 0;
			// Try to read the next batch if rows from the ORC file.
			boolean moreRows = shim.nextBatch(orcRowsReader, rowBatchWrapper.getBatch());

			if (moreRows) {
				// Load the data into the Rows array.
				rowsInBatch = fillRows();
			}
			return moreRows;
		}
		// there is at least one Row left in the Rows array.
		return true;
	}

	@Override
	public void close() throws IOException {
		if (orcRowsReader != null) {
			this.orcRowsReader.close();
		}
		this.orcRowsReader = null;
	}

	// --------------------------------------------------------------------------------------------
	//  Classes to define predicates
	// --------------------------------------------------------------------------------------------

	/**
	 * A filter predicate that can be evaluated by the OrcInputFormat.
	 */
	public abstract static class Predicate implements Serializable {
		public abstract SearchArgument.Builder add(SearchArgument.Builder builder);
	}

	abstract static class ColumnPredicate extends Predicate {
		final String columnName;
		final PredicateLeaf.Type literalType;

		ColumnPredicate(String columnName, PredicateLeaf.Type literalType) {
			this.columnName = columnName;
			this.literalType = literalType;
		}

		Object castLiteral(Serializable literal) {

			switch (literalType) {
				case LONG:
					if (literal instanceof Byte) {
						return new Long((Byte) literal);
					} else if (literal instanceof Short) {
						return new Long((Short) literal);
					} else if (literal instanceof Integer) {
						return new Long((Integer) literal);
					} else if (literal instanceof Long) {
						return literal;
					} else {
						throw new IllegalArgumentException("A predicate on a LONG column requires an integer " +
								"literal, i.e., Byte, Short, Integer, or Long.");
					}
				case FLOAT:
					if (literal instanceof Float) {
						return new Double((Float) literal);
					} else if (literal instanceof Double) {
						return literal;
					} else if (literal instanceof BigDecimal) {
						return ((BigDecimal) literal).doubleValue();
					} else {
						throw new IllegalArgumentException("A predicate on a FLOAT column requires a floating " +
								"literal, i.e., Float or Double.");
					}
				case STRING:
					if (literal instanceof String) {
						return literal;
					} else {
						throw new IllegalArgumentException("A predicate on a STRING column requires a floating " +
								"literal, i.e., Float or Double.");
					}
				case BOOLEAN:
					if (literal instanceof Boolean) {
						return literal;
					} else {
						throw new IllegalArgumentException("A predicate on a BOOLEAN column requires a Boolean literal.");
					}
				case DATE:
					if (literal instanceof Date) {
						return literal;
					} else {
						throw new IllegalArgumentException("A predicate on a DATE column requires a java.sql.Date literal.");
					}
				case TIMESTAMP:
					if (literal instanceof Timestamp) {
						return literal;
					} else {
						throw new IllegalArgumentException("A predicate on a TIMESTAMP column requires a java.sql.Timestamp literal.");
					}
				case DECIMAL:
					if (literal instanceof BigDecimal) {
						return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) literal));
					} else {
						throw new IllegalArgumentException("A predicate on a DECIMAL column requires a BigDecimal literal.");
					}
				default:
					throw new IllegalArgumentException("Unknown literal type " + literalType);
			}
		}
	}

	abstract static class BinaryPredicate extends ColumnPredicate {
		final Serializable literal;

		BinaryPredicate(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
			super(columnName, literalType);
			this.literal = literal;
		}
	}

	/**
	 * An EQUALS predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class Equals extends BinaryPredicate {
		/**
		 * Creates an EQUALS predicate.
		 *
		 * @param columnName The column to check.
		 * @param literalType The type of the literal.
		 * @param literal The literal value to check the column against.
		 */
		public Equals(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
			super(columnName, literalType, literal);
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.equals(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " = " + literal;
		}
	}

	/**
	 * An EQUALS predicate that can be evaluated with Null safety by the OrcInputFormat.
	 */
	public static class NullSafeEquals extends BinaryPredicate {
		/**
		 * Creates a null-safe EQUALS predicate.
		 *
		 * @param columnName The column to check.
		 * @param literalType The type of the literal.
		 * @param literal The literal value to check the column against.
		 */
		public NullSafeEquals(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
			super(columnName, literalType, literal);
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.nullSafeEquals(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " = " + literal;
		}
	}

	/**
	 * A LESS_THAN predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class LessThan extends BinaryPredicate {
		/**
		 * Creates a LESS_THAN predicate.
		 *
		 * @param columnName The column to check.
		 * @param literalType The type of the literal.
		 * @param literal The literal value to check the column against.
		 */
		public LessThan(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
			super(columnName, literalType, literal);
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.lessThan(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " < " + literal;
		}
	}

	/**
	 * A LESS_THAN_EQUALS predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class LessThanEquals extends BinaryPredicate {
		/**
		 * Creates a LESS_THAN_EQUALS predicate.
		 *
		 * @param columnName The column to check.
		 * @param literalType The type of the literal.
		 * @param literal The literal value to check the column against.
		 */
		public LessThanEquals(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
			super(columnName, literalType, literal);
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.lessThanEquals(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " <= " + literal;
		}
	}

	/**
	 * An IS_NULL predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class IsNull extends ColumnPredicate {
		/**
		 * Creates an IS_NULL predicate.
		 *
		 * @param columnName The column to check for null.
		 * @param literalType The type of the column to check for null.
		 */
		public IsNull(String columnName, PredicateLeaf.Type literalType) {
			super(columnName, literalType);
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.isNull(columnName, literalType);
		}

		@Override
		public String toString() {
			return columnName + " IS NULL";
		}
	}

	/**
	 * An BETWEEN predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class Between extends ColumnPredicate {
		private Serializable lowerBound;
		private Serializable upperBound;

		/**
		 * Creates an BETWEEN predicate.
		 *
		 * @param columnName The column to check.
		 * @param literalType The type of the literals.
		 * @param lowerBound The literal value of the (inclusive) lower bound to check the column against.
		 * @param upperBound The literal value of the (inclusive) upper bound to check the column against.
		 */
		public Between(String columnName, PredicateLeaf.Type literalType, Serializable lowerBound, Serializable upperBound) {
			super(columnName, literalType);
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.between(columnName, literalType, castLiteral(lowerBound), castLiteral(upperBound));
		}

		@Override
		public String toString() {
			return lowerBound + " <= " + columnName + " <= " + upperBound;
		}
	}

	/**
	 * An IN predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class In extends ColumnPredicate {
		private Serializable[] literals;

		/**
		 * Creates an IN predicate.
		 *
		 * @param columnName The column to check.
		 * @param literalType The type of the literals.
		 * @param literals The literal values to check the column against.
		 */
		public In(String columnName, PredicateLeaf.Type literalType, Serializable... literals) {
			super(columnName, literalType);
			this.literals = literals;
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			Object[] castedLiterals = new Object[literals.length];
			for (int i = 0; i < literals.length; i++) {
				castedLiterals[i] = castLiteral(literals[i]);
			}
			return builder.in(columnName, literalType, (Object[]) castedLiterals);
		}

		@Override
		public String toString() {
			return columnName + " IN " + Arrays.toString(literals);
		}
	}

	/**
	 * A NOT predicate to negate a predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class Not extends Predicate {
		private final Predicate pred;

		/**
		 * Creates a NOT predicate.
		 *
		 * @param predicate The predicate to negate.
		 */
		public Not(Predicate predicate) {
			this.pred = predicate;
		}

		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			return pred.add(builder.startNot()).end();
		}

		protected Predicate child() {
			return pred;
		}

		@Override
		public String toString() {
			return "NOT(" + pred.toString() + ")";
		}
	}

	/**
	 * An OR predicate that can be evaluated by the OrcInputFormat.
	 */
	public static class Or extends Predicate {
		private final Predicate[] preds;

		/**
		 * Creates an OR predicate.
		 *
		 * @param predicates The disjunctive predicates.
		 */
		public Or(Predicate... predicates) {
			this.preds = predicates;
		}

		@Override
		public SearchArgument.Builder add(SearchArgument.Builder builder) {
			SearchArgument.Builder withOr = builder.startOr();
			for (Predicate p : preds) {
				withOr = p.add(withOr);
			}
			return withOr.end();
		}

		protected Iterable<Predicate> children() {
			return Arrays.asList(preds);
		}

		@Override
		public String toString() {
			return "OR(" + Arrays.toString(preds) + ")";
		}
	}
}
