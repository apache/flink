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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.UdfAnalysisMode;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.java.sca.UdfAnalyzer;
import org.apache.flink.api.java.sca.UdfAnalyzerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UdfOperatorUtils {

	private static final Logger LOG = LoggerFactory.getLogger(UdfOperatorUtils.class);

	public static void analyzeSingleInputUdf(SingleInputUdfOperator<?, ?, ?> operator, Class<?> udfBaseClass,
			Function udf, Keys<?> key) {
		final UdfAnalysisMode mode = operator.getExecutionEnvironment().getConfig().getUdfAnalysisMode();
		if (mode != UdfAnalysisMode.DISABLED) {
			try {
				final UdfAnalyzer analyzer = new UdfAnalyzer(udfBaseClass, udf.getClass(), operator.getInputType(), null,
						operator.getResultType(), key, null, mode == UdfAnalysisMode.OPTIMIZING_ENABLED);
				final boolean success = analyzer.analyze();
				if (success) {
					if (mode == UdfAnalysisMode.OPTIMIZING_ENABLED
							&& !operator.udfWithForwardedFieldsAnnotation(udf.getClass())) {
						operator.setSemanticProperties((SingleInputSemanticProperties) analyzer.getSemanticProperties());
						operator.setAnalyzedUdfSemanticsFlag();
					}
					else if (mode == UdfAnalysisMode.HINTING_ENABLED) {
						analyzer.addSemanticPropertiesHints();
					}
					LOG.info(analyzer.getHintsString());
				}
			}
			catch (InvalidTypesException e) {
				LOG.debug("Unable to do UDF analysis due to missing type information.", e);
			}
			catch (UdfAnalyzerException e) {
				LOG.debug("UDF analysis failed.", e);
			}
		}
	}

	public static void analyzeDualInputUdf(TwoInputUdfOperator<?, ?, ?, ?> operator, Class<?> udfBaseClass,
			Function udf, Keys<?> key1, Keys<?> key2) {
		final UdfAnalysisMode mode = operator.getExecutionEnvironment().getConfig().getUdfAnalysisMode();
		if (mode != UdfAnalysisMode.DISABLED) {
			try {
				final UdfAnalyzer analyzer = new UdfAnalyzer(udfBaseClass, udf.getClass(), operator.getInput1Type(),
						operator.getInput2Type(), operator.getResultType(), key1, key2,
						mode == UdfAnalysisMode.OPTIMIZING_ENABLED);
				final boolean success = analyzer.analyze();
				if (success) {
					if (mode == UdfAnalysisMode.OPTIMIZING_ENABLED
							&& !(operator.udfWithForwardedFieldsFirstAnnotation(udf.getClass())
							|| operator.udfWithForwardedFieldsSecondAnnotation(udf.getClass()))) {
						operator.setSemanticProperties((DualInputSemanticProperties) analyzer.getSemanticProperties());
						operator.setAnalyzedUdfSemanticsFlag();
					}
					else if (mode == UdfAnalysisMode.HINTING_ENABLED) {
						analyzer.addSemanticPropertiesHints();
					}
					LOG.info(analyzer.getHintsString());
				}
			}
			catch (InvalidTypesException e) {
				LOG.debug("Unable to do UDF analysis due to missing type information.", e);
			}
			catch (UdfAnalyzerException e) {
				LOG.debug("UDF analysis failed.", e);
			}
		}
	}
}
