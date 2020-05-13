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

package org.apache.flink.api.java.io.jdbc.dialect;

import org.apache.flink.api.java.io.jdbc.source.row.converter.DerbyRowConverter;
import org.apache.flink.api.java.io.jdbc.source.row.converter.JDBCRowConverter;
import org.apache.flink.api.java.io.jdbc.source.row.converter.MySQLRowConverter;
import org.apache.flink.api.java.io.jdbc.source.row.converter.PostgresRowConverter;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Default JDBC dialects.
 */
public final class JDBCDialects {

	private static final List<JDBCDialect> DIALECTS = Arrays.asList(
		new DerbyDialect(),
		new MySQLDialect(),
		new PostgresDialect()
	);

	/**
	 * Fetch the JDBCDialect class corresponding to a given database url.
	 */
	public static Optional<JDBCDialect> get(String url) {
		for (JDBCDialect dialect : DIALECTS) {
			if (dialect.canHandle(url)) {
				return Optional.of(dialect);
			}
		}
		return Optional.empty();
	}

	private abstract static class AbstractDialect implements JDBCDialect {

		@Override
		public void validate(TableSchema schema) throws ValidationException {
			for (int i = 0; i < schema.getFieldCount(); i++) {
				DataType dt = schema.getFieldDataType(i).get();
				String fieldName = schema.getFieldName(i).get();

				// TODO: We can't convert VARBINARY(n) data type to
				//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter
				//  when n is smaller than Integer.MAX_VALUE
				if (unsupportedTypes().contains(dt.getLogicalType().getTypeRoot()) ||
						(dt.getLogicalType() instanceof VarBinaryType
							&& Integer.MAX_VALUE != ((VarBinaryType) dt.getLogicalType()).getLength())) {
					throw new ValidationException(
							String.format("The %s dialect doesn't support type: %s.",
									dialectName(),
									dt.toString()));
				}

				// only validate precision of DECIMAL type for blink planner
				if (dt.getLogicalType() instanceof DecimalType) {
					int precision = ((DecimalType) dt.getLogicalType()).getPrecision();
					if (precision > maxDecimalPrecision()
							|| precision < minDecimalPrecision()) {
						throw new ValidationException(
								String.format("The precision of field '%s' is out of the DECIMAL " +
												"precision range [%d, %d] supported by %s dialect.",
										fieldName,
										minDecimalPrecision(),
										maxDecimalPrecision(),
										dialectName()));
					}
				}

				// only validate precision of DECIMAL type for blink planner
				if (dt.getLogicalType() instanceof TimestampType) {
					int precision = ((TimestampType) dt.getLogicalType()).getPrecision();
					if (precision > maxTimestampPrecision()
							|| precision < minTimestampPrecision()) {
						throw new ValidationException(
								String.format("The precision of field '%s' is out of the TIMESTAMP " +
												"precision range [%d, %d] supported by %s dialect.",
										fieldName,
										minTimestampPrecision(),
										maxTimestampPrecision(),
										dialectName()));
					}
				}
			}
		}

		public abstract String dialectName();

		public abstract int maxDecimalPrecision();

		public abstract int minDecimalPrecision();

		public abstract int maxTimestampPrecision();

		public abstract int minTimestampPrecision();

		/**
		 * Defines the unsupported types for the dialect.
		 * @return a list of logical type roots.
		 */
		public abstract List<LogicalTypeRoot> unsupportedTypes();
	}

	private static class DerbyDialect extends AbstractDialect {

		private static final long serialVersionUID = 1L;

		// Define MAX/MIN precision of TIMESTAMP type according to derby docs:
		// http://db.apache.org/derby/docs/10.14/ref/rrefsqlj27620.html
		private static final int MAX_TIMESTAMP_PRECISION = 9;
		private static final int MIN_TIMESTAMP_PRECISION = 1;

		// Define MAX/MIN precision of DECIMAL type according to derby docs:
		// http://db.apache.org/derby/docs/10.14/ref/rrefsqlj15260.html
		private static final int MAX_DECIMAL_PRECISION = 31;
		private static final int MIN_DECIMAL_PRECISION = 1;

		// Define MAX length of VARCHAR TYPE  according to derby docs
		// http://db.apache.org/derby/docs/10.14/ref/rrefsqlj41207.html
		private static final int MAX_VARCHAR_LEN = 32672;

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:derby:");
		}

		@Override
		public JDBCRowConverter getRowConverter(RowType rowType) {
			return new DerbyRowConverter(rowType);
		}

		@Override
		public Optional<String> defaultDriverName() {
			return Optional.of("org.apache.derby.jdbc.EmbeddedDriver");
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return identifier;
		}

		@Override
		public String dialectName() {
			return "derby";
		}

		@Override
		public int maxDecimalPrecision() {
			return MAX_DECIMAL_PRECISION;
		}

		@Override
		public int minDecimalPrecision() {
			return MIN_DECIMAL_PRECISION;
		}

		@Override
		public int maxTimestampPrecision() {
			return MAX_TIMESTAMP_PRECISION;
		}

		@Override
		public int minTimestampPrecision() {
			return MIN_TIMESTAMP_PRECISION;
		}

		@Override
		public String getCreateTableStatement(String tableName, TableSchema schema, String[] primaryKeyFields) {
			List<String> expressions = new ArrayList<>();
			for (TableColumn column : schema.getTableColumns()) {
				expressions.add(quoteIdentifier(column.getName()) + " " + getDialectTypeName(column.getType()));
			}
			if (primaryKeyFields != null && primaryKeyFields.length > 0) {
				String primaryKey = "PRIMARY KEY" + String.format("(%s)", StringUtils.join(
					Arrays.stream(primaryKeyFields)
						.map(name -> quoteIdentifier(name))
						.collect(Collectors.toList()),
					","));
				expressions.add(primaryKey);
			}

			// derby do not support `if not exists` grammar
			return String.format("CREATE TABLE %s (%s)", quoteIdentifier(tableName),
				StringUtils.join(expressions, ","));
		}

		@Override
		public List<LogicalTypeRoot> unsupportedTypes() {
			// The data types used in Derby are list at
			// http://db.apache.org/derby/docs/10.14/ref/crefsqlj31068.html

			// TODO: We can't convert BINARY data type to
			//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter.
			return Arrays.asList(
					LogicalTypeRoot.BINARY,
					LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
					LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
					LogicalTypeRoot.INTERVAL_YEAR_MONTH,
					LogicalTypeRoot.INTERVAL_DAY_TIME,
					LogicalTypeRoot.ARRAY,
					LogicalTypeRoot.MULTISET,
					LogicalTypeRoot.MAP,
					LogicalTypeRoot.ROW,
					LogicalTypeRoot.DISTINCT_TYPE,
					LogicalTypeRoot.STRUCTURED_TYPE,
					LogicalTypeRoot.NULL,
					LogicalTypeRoot.RAW,
					LogicalTypeRoot.SYMBOL,
					LogicalTypeRoot.UNRESOLVED);
		}

		@Override
		public String getDialectTypeName(DataType dataType) {
			switch (dataType.getLogicalType().getTypeRoot()) {
				case VARCHAR:
					final int len = ((VarCharType) dataType.getLogicalType()).getLength();
					return String.format("VARCHAR(%d)", len > MAX_VARCHAR_LEN ? MAX_VARCHAR_LEN : len);
				case DECIMAL:
					if (dataType.getLogicalType() instanceof DecimalType) {
						return dataType.toString();
					}
					// for legacy type
					return "DECIMAL(" + MAX_DECIMAL_PRECISION + ", 18)";
				// derby does not support time/timestamp type with precision
				case TIME_WITHOUT_TIME_ZONE:
					return "TIME";
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					return "TIMESTAMP";
				default:
					return dataType.toString();
			}
		}
	}

	/**
	 * MySQL dialect.
	 */
	public static class MySQLDialect extends AbstractDialect {

		private static final long serialVersionUID = 1L;

		// Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
		// https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
		private static final int MAX_TIMESTAMP_PRECISION = 6;
		private static final int MIN_TIMESTAMP_PRECISION = 1;

		// Define MAX/MIN precision of DECIMAL type according to Mysql docs:
		// https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
		private static final int MAX_DECIMAL_PRECISION = 65;
		private static final int MIN_DECIMAL_PRECISION = 1;

		// Define MAX length of VARCHAR type  according to Mysql docs
		// https://dev.mysql.com/doc/refman/8.0/en/char.html
		// The row max length is 65535, set default length of VARCHAR column to 1024.
		private static final int DEFAULT_MAX_VARCHAR_LEN = 1024;

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:mysql:");
		}

		@Override
		public JDBCRowConverter getRowConverter(RowType rowType) {
			return new MySQLRowConverter(rowType);
		}

		@Override
		public Optional<String> defaultDriverName() {
			return Optional.of("com.mysql.jdbc.Driver");
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return "`" + identifier + "`";
		}

		/**
		 * Mysql upsert query use DUPLICATE KEY UPDATE.
		 *
		 * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
		 *
		 * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
		 */
		@Override
		public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
			String updateClause = Arrays.stream(fieldNames)
					.map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
					.collect(Collectors.joining(", "));
			return Optional.of(getInsertIntoStatement(tableName, fieldNames) +
					" ON DUPLICATE KEY UPDATE " + updateClause
			);
		}

		@Override
		public String dialectName() {
			return "mysql";
		}

		@Override
		public int maxDecimalPrecision() {
			return MAX_DECIMAL_PRECISION;
		}

		@Override
		public int minDecimalPrecision() {
			return MIN_DECIMAL_PRECISION;
		}

		@Override
		public int maxTimestampPrecision() {
			return MAX_TIMESTAMP_PRECISION;
		}

		@Override
		public int minTimestampPrecision() {
			return MIN_TIMESTAMP_PRECISION;
		}

		@Override
		public List<LogicalTypeRoot> unsupportedTypes() {
			// The data types used in Mysql are list at:
			// https://dev.mysql.com/doc/refman/8.0/en/data-types.html

			// TODO: We can't convert BINARY data type to
			//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter.
			return Arrays.asList(
					LogicalTypeRoot.BINARY,
					LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
					LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
					LogicalTypeRoot.INTERVAL_YEAR_MONTH,
					LogicalTypeRoot.INTERVAL_DAY_TIME,
					LogicalTypeRoot.ARRAY,
					LogicalTypeRoot.MULTISET,
					LogicalTypeRoot.MAP,
					LogicalTypeRoot.ROW,
					LogicalTypeRoot.DISTINCT_TYPE,
					LogicalTypeRoot.STRUCTURED_TYPE,
					LogicalTypeRoot.NULL,
					LogicalTypeRoot.RAW,
					LogicalTypeRoot.SYMBOL,
					LogicalTypeRoot.UNRESOLVED
			);
		}

		@Override
		public String getDialectTypeName(DataType dataType) {
			switch (dataType.getLogicalType().getTypeRoot()) {
				case VARCHAR:
					final int len = ((VarCharType) dataType.getLogicalType()).getLength();
					return  len > DEFAULT_MAX_VARCHAR_LEN ? "TEXT" : String.format("VARCHAR(%d)", len);
				case DECIMAL:
					if (dataType.getLogicalType() instanceof DecimalType) {
						return dataType.toString();
					}
					// for legacy type
					return "DECIMAL(" + MAX_DECIMAL_PRECISION + ", 18)";
				default:
					return dataType.toString();
			}
		}
	}

	/**
	 * Postgres dialect.
	 */
	public static class PostgresDialect extends AbstractDialect {

		private static final long serialVersionUID = 1L;

		// Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
		// https://www.postgresql.org/docs/12/datatype-datetime.html
		private static final int MAX_TIMESTAMP_PRECISION = 6;
		private static final int MIN_TIMESTAMP_PRECISION = 1;

		// Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
		// https://www.postgresql.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
		private static final int MAX_DECIMAL_PRECISION = 1000;
		private static final int MIN_DECIMAL_PRECISION = 1;

		// Define MAX length of VARCHAR TYPE  according to PostgreSQL docs
		// https://www.postgresql.org/docs/12/datatype-character.html
		// The VARCHAR max length is 1GB bytes, set default length of VARCHAR column to  10 * 1024 * 1024.
		private static final int DEFAULT_MAX_VARCHAR_LEN = 10 * 1024 * 1024;

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:postgresql:");
		}

		@Override
		public JDBCRowConverter getRowConverter(RowType rowType) {
			return new PostgresRowConverter(rowType);
		}

		@Override
		public Optional<String> defaultDriverName() {
			return Optional.of("org.postgresql.Driver");
		}

		/**
		 * Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres.
		 */
		@Override
		public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
			String uniqueColumns = Arrays.stream(uniqueKeyFields)
					.map(this::quoteIdentifier)
					.collect(Collectors.joining(", "));
			String updateClause = Arrays.stream(fieldNames)
					.map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
					.collect(Collectors.joining(", "));
			return Optional.of(getInsertIntoStatement(tableName, fieldNames) +
							" ON CONFLICT (" + uniqueColumns + ")" +
							" DO UPDATE SET " + updateClause
			);
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return identifier;
		}

		@Override
		public String dialectName() {
			return "postgresql";
		}

		@Override
		public int maxDecimalPrecision() {
			return MAX_DECIMAL_PRECISION;
		}

		@Override
		public int minDecimalPrecision() {
			return MIN_DECIMAL_PRECISION;
		}

		@Override
		public int maxTimestampPrecision() {
			return MAX_TIMESTAMP_PRECISION;
		}

		@Override
		public int minTimestampPrecision() {
			return MIN_TIMESTAMP_PRECISION;
		}

		@Override
		public List<LogicalTypeRoot> unsupportedTypes() {
			// The data types used in PostgreSQL are list at:
			// https://www.postgresql.org/docs/12/datatype.html

			// TODO: We can't convert BINARY data type to
			//  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in LegacyTypeInfoDataTypeConverter.
			return Arrays.asList(
					LogicalTypeRoot.BINARY,
					LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
					LogicalTypeRoot.INTERVAL_YEAR_MONTH,
					LogicalTypeRoot.INTERVAL_DAY_TIME,
					LogicalTypeRoot.MULTISET,
					LogicalTypeRoot.MAP,
					LogicalTypeRoot.ROW,
					LogicalTypeRoot.DISTINCT_TYPE,
					LogicalTypeRoot.STRUCTURED_TYPE,
					LogicalTypeRoot.NULL,
					LogicalTypeRoot.RAW,
					LogicalTypeRoot.SYMBOL,
					LogicalTypeRoot.UNRESOLVED
			);
		}

		@Override
		public String getDialectTypeName(DataType dataType) {
			switch (dataType.getLogicalType().getTypeRoot()) {
				case VARCHAR:
					final int len = ((VarCharType) dataType.getLogicalType()).getLength();
					return  len > DEFAULT_MAX_VARCHAR_LEN ? "TEXT" : String.format("VARCHAR(%d)", len);
				// PostgreSQL using `DOUBLE PRECISION` represents double type.
				case DOUBLE:
					return dataType.toString().replace("DOUBLE", "DOUBLE PRECISION");
				case DECIMAL:
					if (dataType.getLogicalType() instanceof DecimalType) {
						return dataType.toString();
					}
					// for legacy type
					return "DECIMAL(" + MAX_DECIMAL_PRECISION + ", 18)";
				default:
					return dataType.toString();
			}
		}
	}
}
