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
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.orc.OrcBatchReader.fillRows;

/**
 * InputFormat to read ORC files.
 */
public class OrcRowInputFormat extends FileInputFormat<Row> implements ResultTypeQueryable<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(OrcRowInputFormat.class);
	// the number of rows read in a batch
	private static final int DEFAULT_BATCH_SIZE = 1000;

	// the number of fields rows to read in a batch
	private int batchSize;
	// the configuration to read with
	private Configuration conf;
	// the schema of the ORC files to read
	private TypeDescription schema;

	// the fields of the ORC schema that the returned Rows are composed of.
	private int[] selectedFields;
	// the type information of the Rows returned by this InputFormat.
	private transient RowTypeInfo rowType;

	// the ORC reader
	private transient RecordReader orcRowsReader;
	// the vectorized row data to be read in a batch
	private transient VectorizedRowBatch rowBatch;
	// the vector of rows that is read in a batch
	private transient Row[] rows;

	// the number of rows in the current batch
	private transient int rowsInBatch;
	// the index of the next row to return
	private transient int nextRow;

	private ArrayList<Predicate> conjunctPredicates = new ArrayList<>();

	/**
	 * Creates an OrcRowInputFormat.
	 *
	 * @param path The path to read ORC files from.
	 * @param schemaString The schema of the ORC files as String.
	 * @param orcConfig The configuration to read the ORC files with.
	 */
	public OrcRowInputFormat(String path, String schemaString, Configuration orcConfig) {
		this(path, TypeDescription.fromString(schemaString), orcConfig, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates an OrcRowInputFormat.
	 *
	 * @param path The path to read ORC files from.
	 * @param schemaString The schema of the ORC files as String.
	 * @param orcConfig The configuration to read the ORC files with.
	 * @param batchSize The number of Row objects to read in a batch.
	 */
	public OrcRowInputFormat(String path, String schemaString, Configuration orcConfig, int batchSize) {
		this(path, TypeDescription.fromString(schemaString), orcConfig, batchSize);
	}

	/**
	 * Creates an OrcRowInputFormat.
	 *
	 * @param path The path to read ORC files from.
	 * @param orcSchema The schema of the ORC files as ORC TypeDescription.
	 * @param orcConfig The configuration to read the ORC files with.
	 * @param batchSize The number of Row objects to read in a batch.
	 */
	public OrcRowInputFormat(String path, TypeDescription orcSchema, Configuration orcConfig, int batchSize) {
		super(new Path(path));

		// configure OrcRowInputFormat
		this.schema = orcSchema;
		this.rowType = (RowTypeInfo) OrcBatchReader.schemaToTypeInfo(schema);
		this.conf = orcConfig;
		this.batchSize = batchSize;

		// set default selection mask, i.e., all fields.
		this.selectedFields = new int[this.schema.getChildren().size()];
		for (int i = 0; i < selectedFields.length; i++) {
			this.selectedFields[i] = i;
		}
	}

	/**
	 * Adds a filter predicate to reduce the number of rows to be returned by the input format.
	 * Multiple conjunctive predicates can be added by calling this method multiple times.
	 *
	 * <p>Note: Predicates can significantly reduce the amount of data that is read.
	 * However, the OrcRowInputFormat does not guarantee that all returned rows qualify the
	 * predicates. Moreover, predicates are only applied if the referenced field is among the
	 * selected fields.
	 *
	 * @param predicate The filter predicate.
	 */
	public void addPredicate(Predicate predicate) {
		// validate
		validatePredicate(predicate);
		// add predicate
		this.conjunctPredicates.add(predicate);
	}

	private void validatePredicate(Predicate pred) {
		if (pred instanceof ColumnPredicate) {
			// check column name
			String colName = ((ColumnPredicate) pred).columnName;
			if (!this.schema.getFieldNames().contains(colName)) {
				throw new IllegalArgumentException("Predicate cannot be applied. " +
					"Column '" + colName + "' does not exist in ORC schema.");
			}
		} else if (pred instanceof Not) {
			validatePredicate(((Not) pred).child());
		} else if (pred instanceof Or) {
			for (Predicate p : ((Or) pred).children()) {
				validatePredicate(p);
			}
		}
	}

	/**
	 * Selects the fields from the ORC schema that are returned by InputFormat.
	 *
	 * @param selectedFields The indices of the fields of the ORC schema that are returned by the InputFormat.
	 */
	public void selectFields(int... selectedFields) {
		// set field mapping
		this.selectedFields = selectedFields;
		// adapt result type
		this.rowType = RowTypeInfo.projectFields(this.rowType, selectedFields);
	}

	/**
	 * Computes the ORC projection mask of the fields to include from the selected fields.rowOrcInputFormat.nextRecord(null).
	 *
	 * @return The ORC projection mask.
	 */
	private boolean[] computeProjectionMask() {
		// mask with all fields of the schema
		boolean[] projectionMask = new boolean[schema.getMaximumId() + 1];
		// for each selected field
		for (int inIdx : selectedFields) {
			// set all nested fields of a selected field to true
			TypeDescription fieldSchema = schema.getChildren().get(inIdx);
			for (int i = fieldSchema.getId(); i <= fieldSchema.getMaximumId(); i++) {
				projectionMask[i] = true;
			}
		}
		return projectionMask;
	}

	@Override
	public void openInputFormat() throws IOException {
		super.openInputFormat();
		// create and initialize the row batch
		this.rows = new Row[batchSize];
		for (int i = 0; i < batchSize; i++) {
			rows[i] = new Row(selectedFields.length);
		}
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {

		LOG.debug("Opening ORC file {}", fileSplit.getPath());

		// open ORC file and create reader
		org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(fileSplit.getPath().getPath());
		Reader orcReader = OrcFile.createReader(hPath, OrcFile.readerOptions(conf));

		// get offset and length for the stripes that start in the split
		Tuple2<Long, Long> offsetAndLength = getOffsetAndLengthForSplit(fileSplit, getStripes(orcReader));

		// create ORC row reader configuration
		Reader.Options options = getOptions(orcReader)
			.schema(schema)
			.range(offsetAndLength.f0, offsetAndLength.f1)
			.useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
			.skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
			.tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));

		// configure filters
		if (!conjunctPredicates.isEmpty()) {
			SearchArgument.Builder b = SearchArgumentFactory.newBuilder();
			b = b.startAnd();
			for (Predicate predicate : conjunctPredicates) {
				predicate.add(b);
			}
			b = b.end();
			options.searchArgument(b.build(), new String[]{});
		}

		// configure selected fields
		options.include(computeProjectionMask());

		// create ORC row reader
		this.orcRowsReader = orcReader.rows(options);

		// assign ids
		this.schema.getId();
		// create row batch
		this.rowBatch = schema.createRowBatch(batchSize);
		rowsInBatch = 0;
		nextRow = 0;
	}

	@VisibleForTesting
	Reader.Options getOptions(Reader orcReader) {
		return orcReader.options();
	}

	@VisibleForTesting
	List<StripeInformation> getStripes(Reader orcReader) {
		return orcReader.getStripes();
	}

	private Tuple2<Long, Long> getOffsetAndLengthForSplit(FileInputSplit split, List<StripeInformation> stripes) {
		long splitStart = split.getStart();
		long splitEnd = splitStart + split.getLength();

		long readStart = Long.MAX_VALUE;
		long readEnd = Long.MIN_VALUE;

		for (StripeInformation s : stripes) {
			if (splitStart <= s.getOffset() && s.getOffset() < splitEnd) {
				// stripe starts in split, so it is included
				readStart = Math.min(readStart, s.getOffset());
				readEnd = Math.max(readEnd, s.getOffset() + s.getLength());
			}
		}

		if (readStart < Long.MAX_VALUE) {
			// at least one split is included
			return Tuple2.of(readStart, readEnd - readStart);
		} else {
			return Tuple2.of(0L, 0L);
		}
	}

	@Override
	public void close() throws IOException {
		if (orcRowsReader != null) {
			this.orcRowsReader.close();
		}
		this.orcRowsReader = null;
	}

	@Override
	public void closeInputFormat() throws IOException {
		this.rows = null;
		this.schema = null;
		this.rowBatch = null;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return !ensureBatch();
	}

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
			boolean moreRows = orcRowsReader.nextBatch(rowBatch);

			if (moreRows) {
				// Load the data into the Rows array.
				rowsInBatch = fillRows(rows, schema, rowBatch, selectedFields);
			}
			return moreRows;
		}
		// there is at least one Row left in the Rows array.
		return true;
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		// return the next row
		return rows[this.nextRow++];
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return rowType;
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeInt(batchSize);
		this.conf.write(out);
		out.writeUTF(schema.toString());

		out.writeInt(selectedFields.length);
		for (int f : selectedFields) {
			out.writeInt(f);
		}

		out.writeInt(conjunctPredicates.size());
		for (Predicate p : conjunctPredicates) {
			out.writeObject(p);
		}
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		batchSize = in.readInt();
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		configuration.readFields(in);

		if (this.conf == null) {
			this.conf = configuration;
		}
		this.schema = TypeDescription.fromString(in.readUTF());

		this.selectedFields = new int[in.readInt()];
		for (int i = 0; i < selectedFields.length; i++) {
			this.selectedFields[i] = in.readInt();
		}

		this.conjunctPredicates = new ArrayList<>();
		int numPreds = in.readInt();
		for (int i = 0; i < numPreds; i++) {
			conjunctPredicates.add((Predicate) in.readObject());
		}
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter methods for tests
	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	Configuration getConfiguration() {
		return conf;
	}

	@VisibleForTesting
	int getBatchSize() {
		return batchSize;
	}

	@VisibleForTesting
	String getSchema() {
		return schema.toString();
	}

	// --------------------------------------------------------------------------------------------
	//  Classes to define predicates
	// --------------------------------------------------------------------------------------------

	/**
	 * A filter predicate that can be evaluated by the OrcRowInputFormat.
	 */
	public abstract static class Predicate implements Serializable {
		protected abstract SearchArgument.Builder add(SearchArgument.Builder builder);
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
	 * An EQUALS predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.equals(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " = " + literal;
		}
	}

	/**
	 * An EQUALS predicate that can be evaluated with Null safety by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.nullSafeEquals(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " = " + literal;
		}
	}

	/**
	 * A LESS_THAN predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.lessThan(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " < " + literal;
		}
	}

	/**
	 * A LESS_THAN_EQUALS predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.lessThanEquals(columnName, literalType, castLiteral(literal));
		}

		@Override
		public String toString() {
			return columnName + " <= " + literal;
		}
	}

	/**
	 * An IS_NULL predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.isNull(columnName, literalType);
		}

		@Override
		public String toString() {
			return columnName + " IS NULL";
		}
	}

	/**
	 * An BETWEEN predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
			return builder.between(columnName, literalType, castLiteral(lowerBound), castLiteral(upperBound));
		}

		@Override
		public String toString() {
			return lowerBound + " <= " + columnName + " <= " + upperBound;
		}
	}

	/**
	 * An IN predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
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
	 * A NOT predicate to negate a predicate that can be evaluated by the OrcRowInputFormat.
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

		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
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
	 * An OR predicate that can be evaluated by the OrcRowInputFormat.
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
		protected SearchArgument.Builder add(SearchArgument.Builder builder) {
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
