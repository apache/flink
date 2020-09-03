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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;
import static org.apache.flink.table.filesystem.RowPartitionComputer.restorePartValueFromType;

/**
 * Orc {@link FileSystemFormatFactory} for file system.
 */
public class OrcFileSystemFormatFactory implements FileSystemFormatFactory {

	private static final Logger LOG = LoggerFactory.getLogger(OrcFileSystemFormatFactory.class);

	public static final String IDENTIFIER = "orc";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return new HashSet<>();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		// support "orc.*"
		return new HashSet<>();
	}

	private static Properties getOrcProperties(ReadableConfig options) {
		Properties orcProperties = new Properties();
		Properties properties = new Properties();
		((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
		properties.forEach((k, v) -> orcProperties.put(IDENTIFIER + "." + k, v));
		return orcProperties;
	}

	private boolean isUnaryValid(CallExpression callExpression) {
		return callExpression.getChildren().size() == 1 && callExpression.getChildren().get(0) instanceof FieldReferenceExpression;
	}

	private boolean isBinaryValid(CallExpression callExpression) {
		return callExpression.getChildren().size() == 2 && ((callExpression.getChildren().get(0) instanceof FieldReferenceExpression && callExpression.getChildren().get(1) instanceof ValueLiteralExpression) ||
				(callExpression.getChildren().get(0) instanceof ValueLiteralExpression && callExpression.getChildren().get(1) instanceof FieldReferenceExpression));
	}

	public OrcSplitReader.Predicate toOrcPredicate(Expression expression) {
		if (expression instanceof CallExpression) {
			CallExpression callExp = (CallExpression) expression;
			FunctionDefinition funcDef = callExp.getFunctionDefinition();

			if (funcDef == BuiltInFunctionDefinitions.IS_NULL || funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL || funcDef == BuiltInFunctionDefinitions.NOT) {
				if (!isUnaryValid(callExp)) {
					// not a valid predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
					return null;
				}

				PredicateLeaf.Type colType = toOrcType(((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType());
				if (colType == null) {
					// unsupported type
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcTableSource.", callExp);
					return null;
				}

				String colName = getColumnName(callExp);

				if (funcDef == BuiltInFunctionDefinitions.IS_NULL) {
					return new OrcSplitReader.IsNull(colName, colType);
				} else if (funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL) {
					return new OrcSplitReader.Not(
							new OrcSplitReader.IsNull(colName, colType));
				} else {
					OrcSplitReader.Predicate c = toOrcPredicate(callExp.getChildren().get(0));
					if (c == null) {
						return null;
					} else {
						return new OrcSplitReader.Not(c);
					}
				}
			} else if (funcDef == BuiltInFunctionDefinitions.OR) {
				if (callExp.getChildren().size() < 2) {
					return null;
				}
				Expression left = callExp.getChildren().get(0);
				Expression right = callExp.getChildren().get(1);

				OrcSplitReader.Predicate c1 = toOrcPredicate(left);
				OrcSplitReader.Predicate c2 = toOrcPredicate(right);
				if (c1 == null || c2 == null) {
					return null;
				} else {
					return new OrcSplitReader.Or(c1, c2);
				}
			} else {
				if (!isBinaryValid(callExp)) {
					// not a valid predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
					return null;
				}

				PredicateLeaf.Type litType = getLiteralType(callExp);
				if (litType == null) {
					// unsupported literal type
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", expression);
					return null;
				}

				boolean literalOnRight = literalOnRight(callExp);
				String colName = getColumnName(callExp);

				// fetch literal and ensure it is serializable
				Object literalObj = getLiteral(callExp).get();
				Serializable literal;
				// validate that literal is serializable
				if (literalObj instanceof Serializable) {
					literal = (Serializable) literalObj;
				} else {
					LOG.warn("Encountered a non-serializable literal of type {}. " +
									"Cannot push predicate [{}] into OrcFileSystemFormatFactory. " +
									"This is a bug and should be reported.",
							literalObj.getClass().getCanonicalName(), expression);
					return null;
				}

				if (funcDef == BuiltInFunctionDefinitions.EQUALS) {
					return new OrcSplitReader.Equals(colName, litType, literal);
				} else if (funcDef == BuiltInFunctionDefinitions.NOT_EQUALS) {
					return new OrcSplitReader.Not(
							new OrcSplitReader.Equals(colName, litType, literal));
				} else if (funcDef == BuiltInFunctionDefinitions.GREATER_THAN) {
					if (literalOnRight) {
						return new OrcSplitReader.Not(
								new OrcSplitReader.LessThanEquals(colName, litType, literal));
					} else {
						return new OrcSplitReader.LessThan(colName, litType, literal);
					}
				} else if (funcDef == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
					if (literalOnRight) {
						return new OrcSplitReader.Not(
								new OrcSplitReader.LessThan(colName, litType, literal));
					} else {
						return new OrcSplitReader.LessThanEquals(colName, litType, literal);
					}
				} else if (funcDef == BuiltInFunctionDefinitions.LESS_THAN) {
					if (literalOnRight) {
						return new OrcSplitReader.LessThan(colName, litType, literal);
					} else {
						return new OrcSplitReader.Not(
								new OrcSplitReader.LessThanEquals(colName, litType, literal));
					}
				} else if (funcDef == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
					if (literalOnRight) {
						return new OrcSplitReader.LessThanEquals(colName, litType, literal);
					} else {
						return new OrcSplitReader.Not(
								new OrcSplitReader.LessThan(colName, litType, literal));
					}
				} else {
					// unsupported predicate
					LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", expression);
					return null;
				}
			}
		} else {
			// unsupported predicate
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", expression);
			return null;
		}
	}

	private String getColumnName(CallExpression comp) {
		if (literalOnRight(comp)) {
			return ((FieldReferenceExpression) comp.getChildren().get(0)).getName();
		} else {
			return ((FieldReferenceExpression) comp.getChildren().get(1)).getName();
		}
	}

	private boolean literalOnRight(CallExpression comp) {
		if (comp.getChildren().size() == 1 && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
			return true;
		} else if (comp.getChildren().get(0) instanceof ValueLiteralExpression
				&& comp.getChildren().get(1) instanceof FieldReferenceExpression) {
			return false;
		} else if (comp.getChildren().get(0) instanceof FieldReferenceExpression
				&& comp.getChildren().get(1) instanceof ValueLiteralExpression) {
			return true;
		} else {
			throw new RuntimeException("Invalid binary comparison.");
		}
	}

	private PredicateLeaf.Type getLiteralType(CallExpression comp) {
		if (literalOnRight(comp)) {
			return toOrcType(((ValueLiteralExpression) comp.getChildren().get(1)).getOutputDataType());
		} else {
			return toOrcType(((ValueLiteralExpression) comp.getChildren().get(0)).getOutputDataType());
		}
	}

	private Optional<?> getLiteral(CallExpression comp) {
		if (literalOnRight(comp)) {
			ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) comp.getChildren().get(1);
			return valueLiteralExpression.getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass());
		} else {
			ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) comp.getChildren().get(0);
			return valueLiteralExpression.getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass());
		}
	}

	public PredicateLeaf.Type toOrcType(DataType type) {
		LogicalTypeRoot ltype = type.getLogicalType().getTypeRoot();

		if (ltype == LogicalTypeRoot.TINYINT ||
				ltype == LogicalTypeRoot.SMALLINT ||
				ltype == LogicalTypeRoot.INTEGER ||
				ltype == LogicalTypeRoot.BIGINT) {
			return PredicateLeaf.Type.LONG;
		} else if (ltype == LogicalTypeRoot.FLOAT ||
				ltype == LogicalTypeRoot.DOUBLE) {
			return PredicateLeaf.Type.FLOAT;
		} else if (ltype == LogicalTypeRoot.BOOLEAN) {
			return PredicateLeaf.Type.BOOLEAN;
		} else if (ltype == LogicalTypeRoot.VARCHAR) {
			return PredicateLeaf.Type.STRING;
		} else if (ltype == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE ||
				ltype == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE ||
				ltype == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) {
			return PredicateLeaf.Type.TIMESTAMP;
		} else if (ltype == LogicalTypeRoot.DATE) {
			return PredicateLeaf.Type.DATE;
		} else if (ltype == LogicalTypeRoot.BINARY) {
			return PredicateLeaf.Type.DECIMAL;
		} else {
			// unsupported type
			return null;
		}
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		ArrayList<OrcSplitReader.Predicate> orcPredicates = new ArrayList<>();

		for (Expression pred : context.getPushedDownFilters()) {
			OrcSplitReader.Predicate orcPred = toOrcPredicate(pred);
			if (orcPred != null) {
				orcPredicates.add(orcPred);
			}
		}

		return new OrcRowDataInputFormat(
				context.getPaths(),
				context.getSchema().getFieldNames(),
				context.getSchema().getFieldDataTypes(),
				context.getProjectFields(),
				context.getDefaultPartName(),
				orcPredicates,
				context.getPushedDownLimit(),
				getOrcProperties(context.getFormatOptions()));
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		LogicalType[] orcTypes = Arrays.stream(context.getFormatFieldTypes())
				.map(DataType::getLogicalType)
				.toArray(LogicalType[]::new);

		TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(
				RowType.of(orcTypes, context.getFormatFieldNames()));

		OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
				new RowDataVectorizer(typeDescription.toString(), orcTypes),
				getOrcProperties(context.getFormatOptions()),
				new Configuration());
		return Optional.of(factory);
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.empty();
	}

	/**
	 * An implementation of {@link FileInputFormat} to read {@link RowData} records
	 * from orc files.
	 */
	public static class OrcRowDataInputFormat extends FileInputFormat<RowData> {

		private static final long serialVersionUID = 1L;

		private final String[] fullFieldNames;
		private final DataType[] fullFieldTypes;
		private final int[] selectedFields;
		private final String partDefaultName;
		private List<OrcSplitReader.Predicate> pushedDownFilters;
		private final Properties properties;
		private final long limit;

		private transient OrcColumnarRowSplitReader<VectorizedRowBatch> reader;
		private transient long currentReadCount;

		public OrcRowDataInputFormat(
				Path[] paths,
				String[] fullFieldNames,
				DataType[] fullFieldTypes,
				int[] selectedFields,
				String partDefaultName,
				List<OrcSplitReader.Predicate> pushedDownFilters,
				long limit,
				Properties properties) {
			super.setFilePaths(paths);
			this.limit = limit;
			this.partDefaultName = partDefaultName;
			this.pushedDownFilters = pushedDownFilters;
			this.fullFieldNames = fullFieldNames;
			this.fullFieldTypes = fullFieldTypes;
			this.selectedFields = selectedFields;
			this.properties = properties;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			// generate partition specs.
			List<String> fieldNameList = Arrays.asList(fullFieldNames);
			LinkedHashMap<String, String> partSpec = PartitionPathUtils.extractPartitionSpecFromPath(
					fileSplit.getPath());
			LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
			partSpec.forEach((k, v) -> partObjects.put(k, restorePartValueFromType(
					partDefaultName.equals(v) ? null : v,
					fullFieldTypes[fieldNameList.indexOf(k)])));

			Configuration conf = new Configuration();
			properties.forEach((k, v) -> conf.set(k.toString(), v.toString()));

			this.reader = OrcSplitReaderUtil.genPartColumnarRowReader(
					"3.1.1", // use the latest hive version
					conf,
					fullFieldNames,
					fullFieldTypes,
					partObjects,
					selectedFields,
					pushedDownFilters,
					DEFAULT_SIZE,
					new Path(fileSplit.getPath().toString()),
					fileSplit.getStart(),
					fileSplit.getLength());
			this.currentReadCount = 0L;
		}

		@Override
		public boolean supportsMultiPaths() {
			return true;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (currentReadCount >= limit) {
				return true;
			} else {
				return reader.reachedEnd();
			}
		}

		@Override
		public RowData nextRecord(RowData reuse) {
			currentReadCount++;
			return reader.nextRecord(reuse);
		}

		@Override
		public void close() throws IOException {
			if (reader != null) {
				this.reader.close();
			}
			this.reader = null;
		}
	}
}
