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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.CodeAnalysisMode;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.sca.CodeAnalyzerException;
import org.apache.flink.api.java.sca.UdfAnalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that contains helper methods to work with UDF operators.
 */
@Internal
public final class UdfOperatorUtils {

	private static final Logger LOG = LoggerFactory.getLogger(UdfOperatorUtils.class);

	public static void analyzeSingleInputUdf(SingleInputUdfOperator<?, ?, ?> operator, Class<?> udfBaseClass,
			String defaultName, Function udf, Keys<?> key) {
		final CodeAnalysisMode mode = operator.getExecutionEnvironment().getConfig().getCodeAnalysisMode();
		if (mode != CodeAnalysisMode.DISABLE
				&& !udf.getClass().isAnnotationPresent(FunctionAnnotation.SkipCodeAnalysis.class)) {
			final String operatorName = operator.getName() != null ? operator.getName()
					: udfBaseClass.getSimpleName() + " at " + defaultName;
			try {
				final UdfAnalyzer analyzer = new UdfAnalyzer(udfBaseClass, udf.getClass(), operatorName, operator.getInputType(), null,
						operator.getResultType(), key, null, mode == CodeAnalysisMode.OPTIMIZE);
				final boolean success = analyzer.analyze();
				if (success) {
					if (mode == CodeAnalysisMode.OPTIMIZE
							&& !operator.udfWithForwardedFieldsAnnotation(udf.getClass())) {
						analyzer.addSemanticPropertiesHints();
						operator.setSemanticProperties((SingleInputSemanticProperties) analyzer.getSemanticProperties());
						operator.setAnalyzedUdfSemanticsFlag();
					}
					else if (mode == CodeAnalysisMode.HINT) {
						analyzer.addSemanticPropertiesHints();
					}
					analyzer.printToLogger(LOG);
				}
			}
			catch (InvalidTypesException e) {
				LOG.debug("Unable to do code analysis due to missing type information.", e);
			}
			catch (CodeAnalyzerException e) {
				LOG.debug("Code analysis failed.", e);
			}
		}
	}

	public static void analyzeDualInputUdf(TwoInputUdfOperator<?, ?, ?, ?> operator, Class<?> udfBaseClass,
			String defaultName, Function udf, Keys<?> key1, Keys<?> key2) {
		final CodeAnalysisMode mode = operator.getExecutionEnvironment().getConfig().getCodeAnalysisMode();
		if (mode != CodeAnalysisMode.DISABLE
				&& !udf.getClass().isAnnotationPresent(FunctionAnnotation.SkipCodeAnalysis.class)) {
			final String operatorName = operator.getName() != null ? operator.getName()
					: udfBaseClass.getSimpleName() + " at " + defaultName;
			try {
				final UdfAnalyzer analyzer = new UdfAnalyzer(udfBaseClass, udf.getClass(), operatorName, operator.getInput1Type(),
						operator.getInput2Type(), operator.getResultType(), key1, key2,
						mode == CodeAnalysisMode.OPTIMIZE);
				final boolean success = analyzer.analyze();
				if (success) {
					if (mode == CodeAnalysisMode.OPTIMIZE
							&& !(operator.udfWithForwardedFieldsFirstAnnotation(udf.getClass())
							|| operator.udfWithForwardedFieldsSecondAnnotation(udf.getClass()))) {
						analyzer.addSemanticPropertiesHints();
						operator.setSemanticProperties((DualInputSemanticProperties) analyzer.getSemanticProperties());
						operator.setAnalyzedUdfSemanticsFlag();
					}
					else if (mode == CodeAnalysisMode.HINT) {
						analyzer.addSemanticPropertiesHints();
					}
					analyzer.printToLogger(LOG);
				}
			}
			catch (InvalidTypesException e) {
				LOG.debug("Unable to do code analysis due to missing type information.", e);
			}
			catch (CodeAnalyzerException e) {
				LOG.debug("Code analysis failed.", e);
			}
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private UdfOperatorUtils() {
		throw new RuntimeException();
	}
}
