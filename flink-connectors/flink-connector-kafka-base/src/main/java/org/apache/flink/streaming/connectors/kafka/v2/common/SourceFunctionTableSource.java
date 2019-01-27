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

package org.apache.flink.streaming.connectors.kafka.v2.common;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableSchemaUtil;

import java.io.Serializable;

import scala.Option;

/**
 * Base class for TableSource wrapper a SourceFunction.
 * @param <OUT>
 */
public class SourceFunctionTableSource<OUT>
		implements BatchTableSource<OUT>, StreamTableSource<OUT>, Serializable {

	private static final long serialVersionUID = 7078000231877526561L;
	private static final int DEFAULT_PARALLELISM = -1;
	public static final String BATCH_TAG = "Batch";
	public static final String STREAM_TAG = "Stream";

	protected TypeInformation<OUT> returnTypeInfo;

	private transient DataStream dataStream;
	private transient DataStreamSource boundedStreamSource;

	private SourceFunction<OUT> sourceFunction;

	public SourceFunctionTableSource() {
	}

	public SourceFunctionTableSource(SourceFunction<OUT> sourceFunction) {
		this.sourceFunction = sourceFunction;
	}

	/**
	 * Returns a source function of the concrete table source for [[DataStream]].
	 */
	public SourceFunction<OUT> getSourceFunction() {
		if (sourceFunction != null) {
			return sourceFunction;
		} else {
			throw new RuntimeException("SourceFunction has not been setup yet");
		}
	}

	@Override
	public String explainSource() {
		return getSourceFunction().toString();
	}

	/**
	 * Get the resource request for the source.
	 *
	 * @return Resource spec need by the source.
	 */
	public ResourceSpec getResource() {
		return null;
	}

	@SuppressWarnings("unchecked")
	public DataStream<OUT> getDataStream(StreamExecutionEnvironment execEnv) {
		// reuse same source
		if (this.dataStream == null) {
			SourceFunction<OUT> sourceFunction = getSourceFunction();
			this.dataStream = execEnv.addSource(sourceFunction)
					.name(String.format("%s-%s", explainSource(), STREAM_TAG));
		}
		return this.dataStream;
	}

	/**
	 * Source connectors support batch mode.
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public DataStream<OUT> getBoundedStream(StreamExecutionEnvironment execEnv) {
		if (this.boundedStreamSource == null) {
			SourceFunction<OUT> sourceFunction = getSourceFunction();
			TypeInformation<OUT> typeInfo = getProducedType();

			int parallelism = DEFAULT_PARALLELISM;
			boundedStreamSource = execEnv.addSource(sourceFunction,
					String.format("%s-%s", explainSource(), BATCH_TAG), typeInfo);
			if (parallelism > 0){
				boundedStreamSource.setParallelism(parallelism);
				boundedStreamSource.getTransformation().setMaxParallelism(parallelism);
			}
			ResourceSpec resource = getResource();
			if (resource != null) {
				boundedStreamSource.getTransformation().setResources(resource, resource);
			}
		}
		return this.boundedStreamSource;
	}

	@Override
	public TableStats getTableStats() {
		return null;
	}

	@Override
	public DataType getReturnType() {
		return new TypeInfoWrappedDataType(getProducedType());
	}

	public TypeInformation<OUT> getProducedType() {
		if (returnTypeInfo != null) {
			return  returnTypeInfo;
		}
		SourceFunction<OUT> sourceFunction = getSourceFunction();
		if (sourceFunction instanceof ResultTypeQueryable) {
			returnTypeInfo = ((ResultTypeQueryable) sourceFunction).getProducedType();
		} else {
			try {
				returnTypeInfo = TypeExtractor.createTypeInfo(
						SourceFunction.class, sourceFunction.getClass(), 0, (TypeInformation) null, (TypeInformation) null);
			} catch (InvalidTypesException var6) {
				throw new RuntimeException("Fail to get type information of source function", var6);
			}
		}
		return returnTypeInfo;
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchemaUtil.fromDataType(getReturnType(), Option.empty());
	}
}
