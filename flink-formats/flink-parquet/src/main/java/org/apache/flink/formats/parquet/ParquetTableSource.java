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

package org.apache.flink.formats.parquet;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.And;
import org.apache.flink.table.expressions.Attribute;
import org.apache.flink.table.expressions.BinaryComparison;
import org.apache.flink.table.expressions.BinaryExpression;
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.GreaterThan;
import org.apache.flink.table.expressions.GreaterThanOrEqual;
import org.apache.flink.table.expressions.LessThan;
import org.apache.flink.table.expressions.LessThanOrEqual;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.Not;
import org.apache.flink.table.expressions.NotEqualTo;
import org.apache.flink.table.expressions.Or;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A TableSource to read Parquet files.
 *
 * <p>The {@link ParquetTableSource} supports projection and filter push-down.</p>
 *
 * <p>An {@link ParquetTableSource} is used as shown in the example below.
 */
public class ParquetTableSource
	implements BatchTableSource<Row>, FilterableTableSource<Row>, ProjectableTableSource<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(ParquetTableSource.class);

	// path to read Parquet files from
	private final String path;
	// schema of the Parquet file
	private final MessageType parquetSchema;
	// the schema of table
	private final TableSchema tableSchema;
	// the configuration to read the file
	private final Configuration parquetConfig;

    // type information of the data returned by the InputFormat
	private final RowTypeInfo typeInfo;
	// list of selected Parquet fields to return
	private final int[] selectedFields;
	// list of predicates to apply
	private final FilterPredicate predicate;
	// flag whether a path is recursively enumerated
	private final boolean recursiveEnumeration;

	private ParquetTableSource(String path, MessageType parquetSchema, Configuration configuration,
									boolean recursiveEnumeration) {
		this(path, parquetSchema, configuration, recursiveEnumeration, null, null);
	}

	private ParquetTableSource(String path, MessageType parquetSchema, Configuration configuration,
									boolean recursiveEnumeration, int[] selectedFields, FilterPredicate predicate) {
		Preconditions.checkNotNull(path, "Path must not be null.");
		Preconditions.checkNotNull(parquetSchema, "ParquetSchema must not be null.");
		Preconditions.checkNotNull(configuration, "Configuration must not be null");
		this.path = path;
		this.parquetSchema = parquetSchema;
		this.parquetConfig = configuration;
		this.selectedFields = selectedFields;
		this.predicate = predicate;
		this.recursiveEnumeration = recursiveEnumeration;

		// determine the type information from the Parquet schema
		RowTypeInfo typeInfoFromSchema = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(parquetSchema);

		// set return type info
		if (selectedFields == null) {
			this.typeInfo = typeInfoFromSchema;
		} else {
			this.typeInfo = RowTypeInfo.projectFields(typeInfoFromSchema, selectedFields);
		}

		// create a TableSchema that corresponds to the Parquet schema
		this.tableSchema = new TableSchema(
			typeInfoFromSchema.getFieldNames(),
			typeInfoFromSchema.getFieldTypes()
		);
	}

	@Override
	public TableSource<Row> projectFields(int[] fields) {
		return new ParquetTableSource(path, parquetSchema, parquetConfig, recursiveEnumeration, fields, null);
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment executionEnvironment) {
		ParquetRowInputFormat parquetRowInputFormat = buildParquetInputFormat();
		parquetRowInputFormat.setNestedFileEnumeration(recursiveEnumeration);
		if (selectedFields != null) {
			parquetRowInputFormat.selectFields(typeInfo.getFieldNames());
		}

		if (predicate != null) {
			parquetRowInputFormat.setFilterPredicate(predicate);
		}
		return executionEnvironment.createInput(parquetRowInputFormat).name(explainSource());
	}

	@Override
	public TableSource<Row> applyPredicate(List<Expression> predicates) {
		FilterPredicate predicate = null;
		if (predicates.size() > 0) {
			predicate = toParquetPredicate(predicates.get(0));

			for (Expression expression : predicates.subList(1, predicates.size())) {
				FilterPredicate right = toParquetPredicate(expression);
				if (predicate != null && right != null) {
					predicate = FilterApi.and(predicate, right);
				} else if (right != null) {
					predicate = right;
				}
			}
		}

		return new ParquetTableSource(path, parquetSchema, this.parquetConfig, recursiveEnumeration, selectedFields, predicate);
	}

	@Override
	public boolean isFilterPushedDown() {
		return predicate != null;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return typeInfo;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String explainSource() {
		return "ParquetFile[path=" + path + ", schema=" + parquetSchema + ", filter=" + predicateString() + "]";
	}

	private String predicateString() {
		if (predicate != null) {
			return predicate.toString();
		} else {
			return "TRUE";
		}
	}

	private FilterPredicate toParquetPredicate(Expression exp) {
		if (exp instanceof Not) {
			FilterPredicate c = toParquetPredicate(((Not) exp).child());
			if (c == null) {
				return null;
			} else {
				return FilterApi.not(c);
			}
		} else if (exp instanceof BinaryComparison) {
			BinaryComparison binComp = (BinaryComparison) exp;

			if (!isValid(binComp)) {
				// unsupported literal Type
				LOG.debug("Unsupported predict [{}] cannot be pushed to ParquetTableSource.", exp);
				return  null;
			}

			Tuple2<Column, Comparable> columnPair = getParquetColumn(binComp);

			if (columnPair != null) {
				if (exp instanceof EqualTo) {
					if (columnPair.f0 instanceof IntColumn) {
						return FilterApi.eq((IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof Operators.LongColumn) {
						return FilterApi.eq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof DoubleColumn) {
						return FilterApi.eq((DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof FloatColumn) {
						return FilterApi.eq((FloatColumn) columnPair.f0, (Float) columnPair.f1);
					} else if (columnPair.f0 instanceof BooleanColumn) {
						return FilterApi.eq((BooleanColumn) columnPair.f0, (Boolean) columnPair.f1);
					} else if (columnPair.f0 instanceof BinaryColumn) {
						return FilterApi.eq((BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
					}
				} else if (exp instanceof NotEqualTo) {
					if (columnPair.f0 instanceof IntColumn) {
						return FilterApi.notEq((IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof LongColumn) {
						return FilterApi.notEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof DoubleColumn) {
						return FilterApi.notEq((DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof FloatColumn) {
						return FilterApi.notEq((FloatColumn) columnPair.f0, (Float) columnPair.f1);
					} else if (columnPair.f0 instanceof BooleanColumn) {
						return FilterApi.notEq((BooleanColumn) columnPair.f0, (Boolean) columnPair.f1);
					} else if (columnPair.f0 instanceof BinaryColumn) {
						return FilterApi.notEq((BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
					}
				} else if (exp instanceof GreaterThan) {
					if (columnPair.f0 instanceof IntColumn) {
						return FilterApi.gt((IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof LongColumn) {
						return FilterApi.gt((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof DoubleColumn) {
						return FilterApi.gt((DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof FloatColumn) {
						return FilterApi.gt((FloatColumn) columnPair.f0, (Float) columnPair.f1);
					}
				} else if (exp instanceof GreaterThanOrEqual) {
					if (columnPair.f0 instanceof IntColumn) {
						return FilterApi.gtEq((IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof LongColumn) {
						return FilterApi.gtEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof DoubleColumn) {
						return FilterApi.gtEq((DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof FloatColumn) {
						return FilterApi.gtEq((FloatColumn) columnPair.f0, (Float) columnPair.f1);
					}
				} else if (exp instanceof LessThan) {
					if (columnPair.f0 instanceof IntColumn) {
						return FilterApi.lt((IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof LongColumn) {
						return FilterApi.lt((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					} else if (columnPair.f0 instanceof DoubleColumn) {
						return FilterApi.lt((DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof FloatColumn) {
						return FilterApi.lt((FloatColumn) columnPair.f0, (Float) columnPair.f1);
					}
				} else if (exp instanceof LessThanOrEqual) {
					if (columnPair.f0 instanceof IntColumn) {
						return FilterApi.ltEq((IntColumn) columnPair.f0, (Integer) columnPair.f1);
					} else if (columnPair.f0 instanceof LongColumn) {
						return FilterApi.ltEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
					}  else if (columnPair.f0 instanceof DoubleColumn) {
						return FilterApi.ltEq((DoubleColumn) columnPair.f0, (Double) columnPair.f1);
					} else if (columnPair.f0 instanceof FloatColumn) {
						return FilterApi.ltEq((FloatColumn) columnPair.f0, (Float) columnPair.f1);
					}
				} else {
					// Unsupported Predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into ParquetTableSource.", exp);
					return null;
				}
			}
		} else if (exp instanceof BinaryExpression) {
			FilterPredicate c1 = toParquetPredicate(((Or) exp).left());
			FilterPredicate c2 = toParquetPredicate(((Or) exp).right());

			if (c1 == null || c2 == null) {
				return null;
			} else {
				if (exp instanceof Or) {
					return FilterApi.or(c1, c2);
				} else if (exp instanceof And) {
					return FilterApi.and(c1, c2);
				} else {
					// Unsupported Predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into ParquetTableSource.", exp);
					return null;
				}
			}
		}

		return null;
	}

	private boolean isValid(BinaryComparison comp) {
		return (comp.left() instanceof Literal && comp.right() instanceof  Attribute) ||
			(comp.left() instanceof Attribute && comp.right() instanceof Literal);
	}

	private boolean literalOnRight(BinaryComparison comp) {
		if (comp.left() instanceof  Literal && comp.right() instanceof  Attribute) {
			return false;
		} else if (comp.left() instanceof Attribute && comp.right() instanceof Literal) {
			return true;
		} else {
			throw new RuntimeException("Invalid binary comparison.");
		}
	}

	private TypeInformation<?> getLiteralType(BinaryComparison comp) {
		if (literalOnRight(comp)) {
			return ((Literal) comp.right()).resultType();
		} else {
			return ((Literal) comp.left()).resultType();
		}
	}

	private Object getLiteral(BinaryComparison comp) {
		if (literalOnRight(comp)) {
			return ((Literal) comp.right()).value();
		} else {
			return ((Literal) comp.left()).value();
		}
	}

	private String getColumnName(BinaryComparison comp) {
		if (literalOnRight(comp)) {
			return ((Attribute) comp.left()).name();
		} else {
			return ((Attribute) comp.right()).name();
		}
	}

	private Tuple2<Column, Comparable> getParquetColumn(BinaryComparison comp) {
		TypeInformation<?> typeInfo = getLiteralType(comp);
		String columnName = getColumnName(comp);

		// fetch literal and ensure it is serializable
		Object value = getLiteral(comp);
		// validate that literal is serializable
		if (!(value instanceof Comparable)) {
			LOG.warn("Encountered a non-comparable literal of type {}." +
				"Cannot push predicate [{}] into ParquetTablesource." +
				"This is a bug and should be reported.", value.getClass().getCanonicalName(), comp);
			return null;
		}

		if (typeInfo == BasicTypeInfo.BYTE_TYPE_INFO ||
			typeInfo == BasicTypeInfo.SHORT_TYPE_INFO ||
			typeInfo == BasicTypeInfo.INT_TYPE_INFO) {
			return new Tuple2<>(FilterApi.intColumn(columnName), (Integer) value);
		} else if (typeInfo == BasicTypeInfo.LONG_TYPE_INFO) {
			return new Tuple2<>(FilterApi.longColumn(columnName), (Long) value);
		} else if (typeInfo == BasicTypeInfo.FLOAT_TYPE_INFO) {
			return new Tuple2<>(FilterApi.floatColumn(columnName), (Float) value);
		} else if (typeInfo == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
			return new Tuple2<>(FilterApi.booleanColumn(columnName), (Boolean) value);
		} else if (typeInfo == BasicTypeInfo.DOUBLE_TYPE_INFO) {
			return new Tuple2<>(FilterApi.doubleColumn(columnName), (Double) value);
		} else if (typeInfo == BasicTypeInfo.STRING_TYPE_INFO) {
			return new Tuple2<>(FilterApi.binaryColumn(columnName), Binary.fromString((String) value));
		} else {
			// unsupported type
			return null;
		}
	}

	@VisibleForTesting
	protected ParquetRowInputFormat buildParquetInputFormat() {
		return new ParquetRowInputFormat(new Path(path), parquetSchema);
	}

	// Builder
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Constructs an {@link ParquetTableSource}.
	 */
	public static class Builder {

		private String path;

		private MessageType schema;

		private Configuration config;

		private boolean recursive = true;


		/**
		 * Sets the path of Parquet files.
		 * If the path is specifies a directory, it will be recursively enumerated.
		 *
		 * @param path the path of the Parquet files.
		 * @return The Builder
		 */
		public Builder path(String path) {
			Preconditions.checkNotNull(path, "Path must not be null");
			Preconditions.checkArgument(!path.isEmpty(), "Path must not be empty");
			this.path = path;
			return this;
		}

		/**
		 * Sets the path of the Parquet files.
		 *
		 * @param path The path of the Parquet files
		 * @param recursive Flag whether to enumerate
		 * @return The Builder
		 */
		public Builder path(String path, boolean recursive) {
			Preconditions.checkNotNull(path, "Path must not be null");
			Preconditions.checkArgument(!path.isEmpty(), "Path must not be empty");
			this.path = path;
			this.recursive = recursive;
			return this;
		}

		/**
		 * Sets the Parquet schema of the files to read as a String.
		 *
		 * @param parquetSchema The parquet schema of the files to read as a String.
		 * @return The Builder
		 */
		public Builder forParquetSchema(MessageType parquetSchema) {
			Preconditions.checkNotNull(parquetSchema, "Parquet schema must not be null");
			this.schema = parquetSchema;
			return this;
		}

		/**
		 * Sets a Hadoop {@link Configuration} for the Parquet Reader. If no configuration is configured,
		 * an empty configuration is used.
		 *
		 * @param config The Hadoop Configuration for the Parquet reader.
		 * @return The Builder
		 */
		public Builder withConfiguration(Configuration config) {
			Preconditions.checkNotNull(config, "Configuration must not be null.");
			this.config = config;
			return this;
		}

		/**
		 * Builds the ParquetTableSource for this builder.
		 *
		 * @return The ParquetTableSource for this builder.
		 */
		public ParquetTableSource build() {
			Preconditions.checkNotNull(path, "Path must not be null");
			Preconditions.checkNotNull(schema, "Parquet schema must not be null");
			if (config == null) {
				this.config = new Configuration();
			}

			return new ParquetTableSource(this.path, this.schema, this.config, this.recursive);
		}
	}
}
