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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.orc.OrcRowInputFormat.Predicate;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinitions;
import org.apache.flink.table.expressions.FunctionType;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A TableSource to read ORC files.
 *
 * <p>The {@link OrcTableSource} supports projection and filter push-down.</p>
 *
 * <p>An {@link OrcTableSource} is used as shown in the example below.
 *
 * <pre>
 * {@code
 * OrcTableSource orcSrc = OrcTableSource.builder()
 *   .path("file:///my/data/file.orc")
 *   .forOrcSchema("struct<col1:boolean,col2:tinyint,col3:smallint,col4:int>")
 *   .build();
 *
 * tEnv.registerTableSource("orcTable", orcSrc);
 * Table res = tableEnv.sqlQuery("SELECT * FROM orcTable");
 * }
 * </pre>
 */
public class OrcTableSource
	implements BatchTableSource<Row>, ProjectableTableSource<Row>, FilterableTableSource<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(OrcTableSource.class);

	private static final int DEFAULT_BATCH_SIZE = 1000;

	// path to read ORC files from
	private final String path;
	// schema of the ORC file
	private final TypeDescription orcSchema;
	// the schema of the Table
	private final TableSchema tableSchema;
	// the configuration to read the file
	private final Configuration orcConfig;
	// the number of rows to read in a batch
	private final int batchSize;
	// flag whether a path is recursively enumerated
	private final boolean recursiveEnumeration;

	// type information of the data returned by the InputFormat
	private final RowTypeInfo typeInfo;
	// list of selected ORC fields to return
	private final int[] selectedFields;
	// list of predicates to apply
	private final Predicate[] predicates;

	/**
	 * Creates an OrcTableSouce from an ORC TypeDescription.
	 *
	 * @param path		The path to read the ORC files from.
	 * @param orcSchema The schema of the ORC files as TypeDescription.
	 * @param orcConfig The configuration to read the ORC files.
	 * @param batchSize The number of Rows to read in a batch, default is 1000.
	 * @param recursiveEnumeration Flag whether the path should be recursively enumerated or not.
	 */
	private OrcTableSource(String path, TypeDescription orcSchema, Configuration orcConfig, int batchSize, boolean recursiveEnumeration) {
		this(path, orcSchema, orcConfig, batchSize, recursiveEnumeration, null, null);
	}

	private OrcTableSource(String path, TypeDescription orcSchema, Configuration orcConfig,
							int batchSize, boolean recursiveEnumeration,
							int[] selectedFields, Predicate[] predicates) {

		Preconditions.checkNotNull(path, "Path must not be null.");
		Preconditions.checkNotNull(orcSchema, "OrcSchema must not be null.");
		Preconditions.checkNotNull(path, "Configuration must not be null.");
		Preconditions.checkArgument(batchSize > 0, "Batch size must be larger than null.");
		this.path = path;
		this.orcSchema = orcSchema;
		this.orcConfig = orcConfig;
		this.batchSize = batchSize;
		this.recursiveEnumeration = recursiveEnumeration;
		this.selectedFields = selectedFields;
		this.predicates = predicates;

		// determine the type information from the ORC schema
		RowTypeInfo typeInfoFromSchema = (RowTypeInfo) OrcBatchReader.schemaToTypeInfo(this.orcSchema);

		// set return type info
		if (selectedFields == null) {
			this.typeInfo = typeInfoFromSchema;
		} else {
			this.typeInfo = RowTypeInfo.projectFields(typeInfoFromSchema, selectedFields);
		}

		// create a TableSchema that corresponds to the ORC schema
		this.tableSchema = new TableSchema(
			typeInfoFromSchema.getFieldNames(),
			typeInfoFromSchema.getFieldTypes()
		);
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		OrcRowInputFormat orcIF = buildOrcInputFormat();
		orcIF.setNestedFileEnumeration(recursiveEnumeration);
		if (selectedFields != null) {
			orcIF.selectFields(selectedFields);
		}
		if (predicates != null) {
			for (OrcRowInputFormat.Predicate pred : predicates) {
				orcIF.addPredicate(pred);
			}
		}
		return execEnv.createInput(orcIF).name(explainSource());
	}

	@VisibleForTesting
	protected OrcRowInputFormat buildOrcInputFormat() {
		return new OrcRowInputFormat(path, orcSchema, orcConfig, batchSize);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return typeInfo;
	}

	@Override
	public TableSchema getTableSchema() {
		return this.tableSchema;
	}

	@Override
	public TableSource<Row> projectFields(int[] selectedFields) {
		// create a copy of the OrcTableSouce with new selected fields
		return new OrcTableSource(path, orcSchema, orcConfig, batchSize, recursiveEnumeration, selectedFields, predicates);
	}

	@Override
	public TableSource<Row> applyPredicate(List<Expression> predicates) {
		ArrayList<Predicate> orcPredicates = new ArrayList<>();

		// we do not remove any predicates from the list because ORC does not fully apply predicates
		for (Expression pred : predicates) {
			Predicate orcPred = toOrcPredicate(pred);
			if (orcPred != null) {
				LOG.info("Predicate [{}] converted into OrcPredicate [{}] and pushed into OrcTableSource for path {}.", pred, orcPred, path);
				orcPredicates.add(orcPred);
			} else {
				LOG.info("Predicate [{}] could not be pushed into OrcTableSource for path {}.", pred, path);
			}
		}

		return new OrcTableSource(path, orcSchema, orcConfig, batchSize, recursiveEnumeration, selectedFields, orcPredicates.toArray(new Predicate[]{}));
	}

	@Override
	public boolean isFilterPushedDown() {
		return this.predicates != null;
	}

	@Override
	public String explainSource() {
		return "OrcFile[path=" + path + ", schema=" + orcSchema + ", filter=" + predicateString() + "]";
	}

	private String predicateString() {
		if (predicates != null) {
			return "AND(" + Arrays.toString(predicates) + ")";
		} else {
			return "TRUE";
		}
	}

	// Predicate conversion for filter push-down.

	private Predicate toOrcPredicate(Expression pred) {
		if (pred instanceof CallExpression) {
			CallExpression predCall = (CallExpression) pred;
			if (predCall.getFunctionDefinition() == FunctionDefinitions.OR) {
				Predicate c1 = toOrcPredicate(predCall.getChildren().get(0));
				Predicate c2 = toOrcPredicate(predCall.getChildren().get(1));
				if (c1 == null || c2 == null) {
					return null;
				} else {
					return new OrcRowInputFormat.Or(c1, c2);
				}
			} else if (predCall.getFunctionDefinition() == FunctionDefinitions.NOT) {
				Predicate c = toOrcPredicate(predCall.getChildren().get(0));
				if (c == null) {
					return null;
				} else {
					return new OrcRowInputFormat.Not(c);
				}
			} else if (predCall.getFunctionDefinition().getFunctionType() == FunctionType.BINARY_COMPARISON) {
				Expression left = predCall.getChildren().get(0);
				Expression right = predCall.getChildren().get(1);

				if (!isValid(left, right)) {
					// not a valid predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcTableSource.", pred);
					return null;
				}
				PredicateLeaf.Type litType = getLiteralType(left, right);
				if (litType == null) {
					// unsupported literal type
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcTableSource.", pred);
					return null;
				}

				boolean literalOnRight = literalOnRight(left, right);
				String colName = getColumnName(left, right);

				// fetch literal and ensure it is serializable
				Object literalObj = getLiteral(left, right);
				Serializable literal;
				// validate that literal is serializable
				if (literalObj instanceof Serializable) {
					literal = (Serializable) literalObj;
				} else {
					LOG.warn("Encountered a non-serializable literal of type {}. " +
							"Cannot push predicate [{}] into OrcTableSource. " +
							"This is a bug and should be reported.",
							literalObj.getClass().getCanonicalName(), pred);
					return null;
				}

				if (predCall.getFunctionDefinition() == FunctionDefinitions.EQUALS) {
					return new OrcRowInputFormat.Equals(colName, litType, literal);
				} else if (predCall.getFunctionDefinition() == FunctionDefinitions.NOT_EQUALS) {
					return new OrcRowInputFormat.Not(
						new OrcRowInputFormat.Equals(colName, litType, literal));
				} else if (predCall.getFunctionDefinition() == FunctionDefinitions.GREATER_THAN) {
					if (literalOnRight) {
						return new OrcRowInputFormat.Not(
							new OrcRowInputFormat.LessThanEquals(colName, litType, literal));
					} else {
						return new OrcRowInputFormat.LessThan(colName, litType, literal);
					}
				} else if (predCall.getFunctionDefinition() == FunctionDefinitions.GREATER_THAN_OR_EQUAL) {
					if (literalOnRight) {
						return new OrcRowInputFormat.Not(
							new OrcRowInputFormat.LessThan(colName, litType, literal));
					} else {
						return new OrcRowInputFormat.LessThanEquals(colName, litType, literal);
					}
				} else if (predCall.getFunctionDefinition() == FunctionDefinitions.LESS_THAN) {
					if (literalOnRight) {
						return new OrcRowInputFormat.LessThan(colName, litType, literal);
					} else {
						return new OrcRowInputFormat.Not(
							new OrcRowInputFormat.LessThanEquals(colName, litType, literal));
					}
				} else if (predCall.getFunctionDefinition() == FunctionDefinitions.LESS_THAN_OR_EQUAL) {
					if (literalOnRight) {
						return new OrcRowInputFormat.LessThanEquals(colName, litType, literal);
					} else {
						return new OrcRowInputFormat.Not(
							new OrcRowInputFormat.LessThan(colName, litType, literal));
					}
				}
			} else if (predCall.getFunctionDefinition().getFunctionType() == FunctionType.UNARY_COMPARISON) {

				if (predCall.getChildren().size() != 1 || !isValid(predCall.getChildren().get(0))) {
					// not a valid predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcTableSource.", pred);
					return null;
				}
				PredicateLeaf.Type colType =
					toOrcType(((FieldReferenceExpression) (predCall.getChildren().get(0))).getResultType().get());
				if (colType == null) {
					// unsupported type
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcTableSource.", pred);
					return null;
				}

				String colName = getColumnName(predCall.getChildren().get(0));

				if (predCall.getFunctionDefinition() == FunctionDefinitions.IS_NULL) {
					return new OrcRowInputFormat.IsNull(colName, colType);
				} else if (predCall.getFunctionDefinition() == FunctionDefinitions.IS_NOT_NULL) {
					return new OrcRowInputFormat.Not(
						new OrcRowInputFormat.IsNull(colName, colType));
				}
			}
		}

		// unsupported predicate
		LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcTableSource.", pred);
		return null;
	}

	private boolean isValid(Expression expression) {
		return expression instanceof FieldReferenceExpression;
	}

	private boolean isValid(Expression left, Expression right) {
		return (left instanceof ValueLiteralExpression && right instanceof FieldReferenceExpression) ||
			(left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression);
	}

	private boolean literalOnRight(Expression left, Expression right) {
		if (left instanceof ValueLiteralExpression && right instanceof FieldReferenceExpression) {
			return false;
		} else if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
			return true;
		} else {
			throw new RuntimeException("Invalid binary comparison.");
		}
	}

	private String getColumnName(Expression expression) {
		return ((FieldReferenceExpression) expression).getName();
	}

	private String getColumnName(Expression left, Expression right) {
		if (literalOnRight(left, right)) {
			return ((FieldReferenceExpression) left).getName();
		} else {
			return ((FieldReferenceExpression) right).getName();
		}
	}

	private PredicateLeaf.Type getLiteralType(Expression left, Expression right) {
		if (literalOnRight(left, right)) {
			return toOrcType(((ValueLiteralExpression) right).getType().get());
		} else {
			return toOrcType(((ValueLiteralExpression) left).getType().get());
		}
	}

	private Object getLiteral(Expression left, Expression right) {
		if (literalOnRight(left, right)) {
			return ((ValueLiteralExpression) right).getValue();
		} else {
			return ((ValueLiteralExpression) left).getValue();
		}
	}

	private PredicateLeaf.Type toOrcType(TypeInformation<?> type) {
		if (type == BasicTypeInfo.BYTE_TYPE_INFO ||
			type == BasicTypeInfo.SHORT_TYPE_INFO ||
			type == BasicTypeInfo.INT_TYPE_INFO ||
			type == BasicTypeInfo.LONG_TYPE_INFO) {
			return PredicateLeaf.Type.LONG;
		} else if (type == BasicTypeInfo.FLOAT_TYPE_INFO ||
			type == BasicTypeInfo.DOUBLE_TYPE_INFO) {
			return PredicateLeaf.Type.FLOAT;
		} else if (type == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
			return PredicateLeaf.Type.BOOLEAN;
		} else if (type == BasicTypeInfo.STRING_TYPE_INFO) {
			return PredicateLeaf.Type.STRING;
		} else if (type == SqlTimeTypeInfo.TIMESTAMP) {
			return PredicateLeaf.Type.TIMESTAMP;
		} else if (type == SqlTimeTypeInfo.DATE) {
			return PredicateLeaf.Type.DATE;
		} else if (type == BasicTypeInfo.BIG_DEC_TYPE_INFO) {
			return PredicateLeaf.Type.DECIMAL;
		} else {
			// unsupported type
			return null;
		}
	}

	// Builder

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Constructs an {@link OrcTableSource}.
	 */
	public static class Builder {

		private String path;

		private TypeDescription schema;

		private Configuration config;

		private int batchSize = 0;

		private boolean recursive = true;

		/**
		 * Sets the path of the ORC file(s).
		 * If the path specifies a directory, it will be recursively enumerated.
		 *
		 * @param path The path of the ORC file(s).
		 * @return The builder.
		 */
		public Builder path(String path) {
			Preconditions.checkNotNull(path, "Path must not be null.");
			Preconditions.checkArgument(!path.isEmpty(), "Path must not be empty.");
			this.path = path;
			return this;
		}

		/**
		 * Sets the path of the ORC file(s).
		 *
		 * @param path The path of the ORC file(s).
		 * @param recursive Flag whether the to enumerate
		 * @return The builder.
		 */
		public Builder path(String path, boolean recursive) {
			Preconditions.checkNotNull(path, "Path must not be null.");
			Preconditions.checkArgument(!path.isEmpty(), "Path must not be empty.");
			this.path = path;
			this.recursive = recursive;
			return this;
		}

		/**
		 * Sets the ORC schema of the files to read as a String.
		 *
		 * @param orcSchema The ORC schema of the files to read as a String.
		 * @return The builder.
		 */
		public Builder forOrcSchema(String orcSchema) {
			Preconditions.checkNotNull(orcSchema, "ORC schema must not be null.");
			this.schema = TypeDescription.fromString(orcSchema);
			return this;
		}

		/**
		 * Sets the ORC schema of the files to read as a {@link TypeDescription}.
		 *
		 * @param orcSchema The ORC schema of the files to read as a String.
		 * @return The builder.
		 */
		public Builder forOrcSchema(TypeDescription orcSchema) {
			Preconditions.checkNotNull(orcSchema, "ORC Schema must not be null.");
			this.schema = orcSchema;
			return this;
		}

		/**
		 * Sets a Hadoop {@link Configuration} for the ORC reader. If no configuration is configured,
		 * an empty configuration is used.
		 *
		 * @param config The Hadoop Configuration for the ORC reader.
		 * @return The builder.
		 */
		public Builder withConfiguration(Configuration config) {
			Preconditions.checkNotNull(config, "Configuration must not be null.");
			this.config = config;
			return this;
		}

		/**
		 * Sets the number of rows that are read in a batch. If not configured, the ORC files are
		 * read with a batch size of 1000.
		 *
		 * @param batchSize The number of rows that are read in a batch.
		 * @return The builder.
		 */
		public Builder withBatchSize(int batchSize) {
			Preconditions.checkArgument(batchSize > 0, "Batch size must be greater than zero.");
			this.batchSize = batchSize;
			return this;
		}

		/**
		 * Builds the OrcTableSource for this builder.
		 *
		 * @return The OrcTableSource for this builder.
		 */
		public OrcTableSource build() {
			Preconditions.checkNotNull(this.path, "Path must not be null.");
			Preconditions.checkNotNull(this.schema, "ORC schema must not be null.");
			if (this.config == null) {
				this.config = new Configuration();
			}
			if (this.batchSize == 0) {
				// set default batch size
				this.batchSize = DEFAULT_BATCH_SIZE;
			}
			return new OrcTableSource(this.path, this.schema, this.config, this.batchSize, this.recursive);
		}

	}

}
