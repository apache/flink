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

package org.apache.flink.api.java.sca;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.sca.TaggedValue.Input;

import org.apache.flink.shaded.asm5.org.objectweb.asm.Type;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.MethodNode;

import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.convertTypeInfoToTaggedValue;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.findMethodNode;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.mergeReturnValues;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.removeUngroupedInputsFromContainer;

/**
 * Implements a Static Code Analyzer (SCA) that uses the ASM framework
 * for interpreting Java bytecode of Flink UDFs. The analyzer is build on
 * top of ASM's BasicInterpreter. Instead of ASM's BasicValues, it introduces
 * TaggedValues which extend BasicValue and allows for appending interesting
 * information to values. Interesting values such as inputs, collectors, or
 * constants are tagged such that a tracking of atomic input fields through the
 * entire UDF (until the function returns or calls collect()) is possible.
 *
 * <p>The implementation is as conservative as possible meaning that for cases
 * or bytecode instructions that haven't been considered the analyzer
 * will fallback to the ASM library (which removes TaggedValues).
 */
@Internal
public class UdfAnalyzer {
	// necessary to prevent endless loops and stack overflows
	private static final int MAX_NESTING = 20;

	// general information about the UDF that is available before analysis takes place
	private final Method baseClassMethod;
	private final boolean hasCollector;
	private final boolean isBinary;
	private final boolean isIterableInput;
	private final boolean isReduceFunction;
	private final boolean isFilterFunction;
	private final Class<?> udfClass;
	private final String externalUdfName;
	private final String internalUdfClassName;
	private final TypeInformation<?> in1Type;
	private final TypeInformation<?> in2Type;
	private final TypeInformation<?> outType;
	private final Keys<?> keys1;
	private final Keys<?> keys2;

	// flag if code errors should throw an CodeErrorException
	private final boolean throwErrorExceptions;

	// list of all values added with a "collect()" call
	private final List<TaggedValue> collectorValues;

	// list of all hints that can be returned after analysis
	private final List<String> hints;
	private boolean warning = false;

	// stages for capturing and tagging the initial BasicValues
	private int state = STATE_CAPTURE_RETURN;

	static final int STATE_CAPTURE_RETURN = 0;
	static final int STATE_CAPTURE_THIS = 1;
	static final int STATE_CAPTURE_INPUT1 = 2;
	static final int STATE_CAPTURE_INPUT2 = 3;
	static final int STATE_CAPTURE_COLLECTOR = 4;
	static final int STATE_END_OF_CAPTURING = 5;
	static final int STATE_END_OF_ANALYZING = 6;

	// flag that indicates if the "hasNext()" call of an input iterator has returned "TRUE"
	// and can now return "FALSE" if assumption has already been used
	private boolean iteratorTrueAssumptionApplied;

	// merged return value of all return statements in the UDF
	private TaggedValue returnValue;

	// statistics for object creation hinting
	private int newOperationCounterOverall;
	private int newOperationCounterTopLevel;

	// stored FilterFunction input for later modification checking
	private TaggedValue filterInputCopy;
	private TaggedValue filterInputRef;

	public UdfAnalyzer(Class<?> baseClass, Class<?> udfClass, String externalUdfName,
			TypeInformation<?> in1Type, TypeInformation<?> in2Type,
			TypeInformation<?> outType, Keys<?> keys1, Keys<?> keys2,
			boolean throwErrorExceptions) {

		baseClassMethod = baseClass.getDeclaredMethods()[0];
		this.udfClass = udfClass;
		this.externalUdfName = externalUdfName;
		this.internalUdfClassName = Type.getInternalName(udfClass);
		this.in1Type = in1Type;
		this.in2Type = in2Type;
		this.outType = outType;
		this.keys1 = keys1;
		this.keys2 = keys2;
		this.throwErrorExceptions = throwErrorExceptions;

		if (baseClass == CoGroupFunction.class) {
			hasCollector = true;
			isBinary = true;
			isIterableInput = true;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == CrossFunction.class) {
			hasCollector = false;
			isBinary = true;
			isIterableInput = false;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == FlatJoinFunction.class) {
			hasCollector = true;
			isBinary = true;
			isIterableInput = false;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == FlatMapFunction.class) {
			hasCollector = true;
			isBinary = false;
			isIterableInput = false;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == GroupReduceFunction.class) {
			hasCollector = true;
			isBinary = false;
			isIterableInput = true;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = false;
		}
		else if (baseClass == JoinFunction.class) {
			hasCollector = false;
			isBinary = true;
			isIterableInput = false;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == MapFunction.class) {
			hasCollector = false;
			isBinary = false;
			isIterableInput = false;
			isReduceFunction = false;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == ReduceFunction.class) {
			hasCollector = false;
			isBinary = false;
			isIterableInput = false;
			isReduceFunction = true;
			isFilterFunction = false;
			iteratorTrueAssumptionApplied = true;
		}
		else if (baseClass == FilterFunction.class) {
			hasCollector = false;
			isBinary = false;
			isIterableInput = false;
			isReduceFunction = false;
			isFilterFunction = true;
			iteratorTrueAssumptionApplied = true;
		}
		// TODO MapPartitionFunction, GroupCombineFunction and CombineFunction not implemented yet
		else {
			throw new UnsupportedOperationException("Unsupported operator.");
		}
		if (hasCollector) {
			collectorValues = new ArrayList<TaggedValue>();
		}
		else {
			collectorValues = null;
		}
		hints = new ArrayList<String>();
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public boolean isUdfBinary() {
		return isBinary;
	}

	public boolean isIteratorTrueAssumptionApplied() {
		return iteratorTrueAssumptionApplied;
	}

	public void applyIteratorTrueAssumption() {
		iteratorTrueAssumptionApplied = true;
	}

	public void incrNewOperationCounters(boolean topLevel) {
		newOperationCounterOverall++;
		if (topLevel) {
			newOperationCounterTopLevel++;
		}
	}

	public boolean hasUdfCollector() {
		return hasCollector;
	}

	public boolean hasUdfIterableInput() {
		return isIterableInput;
	}

	public boolean isUdfReduceFunction() {
		return isReduceFunction;
	}

	public String getInternalUdfClassName() {
		return internalUdfClassName;
	}

	public List<TaggedValue> getCollectorValues() {
		return collectorValues;
	}

	public boolean analyze() throws CodeAnalyzerException {
		if (state == STATE_END_OF_ANALYZING) {
			throw new IllegalStateException("Analyzing is already done.");
		}

		boolean discardReturnValues = false;

		if (isIterableInput) {
			if (keys1 == null || (keys2 == null && isBinary)) {
				throw new IllegalArgumentException("This type of function requires key information for analysis.");
			}
			else if (!(keys1 instanceof ExpressionKeys) || (!(keys2 instanceof ExpressionKeys) && isBinary)) {
				// TODO currently only ExpressionKeys are supported as keys
				discardReturnValues = true;
			}
		}

		try {
			final Object[] mn = findMethodNode(internalUdfClassName, baseClassMethod);
			final NestedMethodAnalyzer nma = new NestedMethodAnalyzer(this, (String) mn[1],
					(MethodNode) mn[0], null, MAX_NESTING, true);
			final TaggedValue result = nma.analyze();
			setState(STATE_END_OF_ANALYZING);

			// special case: FilterFunction
			if (isFilterFunction) {
				discardReturnValues = true;
				// check for input modification
				if (!filterInputCopy.equals(filterInputRef)) {
					addHintOrThrowException("Function modifies the input. This can lead to unexpected behaviour during runtime.");
				}
			}

			if (!discardReturnValues) {
				// merge return values of a collector
				if (hasCollector) {
					returnValue = mergeReturnValues(collectorValues);
				}
				else {
					returnValue = result;
				}
				// remove ungrouped inputs from result if UDF has iterators
				// or is a reduce function
				if ((isIterableInput || isReduceFunction) && returnValue != null) {
					if (returnValue.canContainFields()) {
						removeUngroupedInputsFromContainer(returnValue);
					}
					else if (returnValue.isInput() && !returnValue.isGrouped()) {
						returnValue = null;
					}
				}
			}
			// any return value is invalid
			else {
				returnValue = null;
			}
		}
		catch (Exception e) {
			Throwable cause = e.getCause();
			while (cause != null && !(cause instanceof CodeErrorException)) {
				cause = cause.getCause();
			}
			if (cause instanceof CodeErrorException || e instanceof CodeErrorException) {
				throw new CodeErrorException("Function code contains obvious errors. " +
						"If you think the code analysis is wrong at this point you can " +
						"disable the entire code analyzer in ExecutionConfig or add" +
						" @SkipCodeAnalysis to your function to disable the analysis.",
						(cause != null) ? cause : e);
			}
			throw new CodeAnalyzerException("Exception occurred during code analysis.", e);
		}
		return true;
	}

	public SemanticProperties getSemanticProperties() {
		final SemanticProperties sp;
		if (isBinary) {
			sp = new DualInputSemanticProperties();
			if (returnValue != null) {
				String[] ff1Array = null;
				final String ff1 = returnValue.toForwardedFieldsExpression(Input.INPUT_1);
				if (ff1 != null && ff1.length() > 0) {
					ff1Array = new String[]{ff1};
				}
				String[] ff2Array = null;
				final String ff2 = returnValue.toForwardedFieldsExpression(Input.INPUT_2);
				if (ff2 != null && ff2.length() > 0) {
					ff2Array = new String[]{ff2};
				}
				SemanticPropUtil.getSemanticPropsDualFromString((DualInputSemanticProperties) sp,
						ff1Array, ff2Array, null, null, null, null, in1Type, in2Type, outType, true);
			}
		}
		else {
			sp = new SingleInputSemanticProperties();
			if (returnValue != null) {
				String[] ffArray = null;
				final String ff = returnValue.toForwardedFieldsExpression(Input.INPUT_1);
				if (ff != null && ff.length() > 0) {
					ffArray = new String[]{ff};
				}
				SemanticPropUtil.getSemanticPropsSingleFromString((SingleInputSemanticProperties) sp,
						ffArray, null, null, in1Type, outType, true);
			}
		}
		return sp;
	}

	public void addSemanticPropertiesHints() {
		boolean added = false;
		if (returnValue != null) {
			if (isBinary) {
				final String ff1 = returnValue.toForwardedFieldsExpression(Input.INPUT_1);
				if (ff1 != null && ff1.length() > 0) {
					added = true;
					hints.add("Possible annotation: "
							+ "@ForwardedFieldsFirst(\"" + ff1 + "\")");
				}
				final String ff2 = returnValue.toForwardedFieldsExpression(Input.INPUT_2);
				if (ff2 != null && ff2.length() > 0) {
					added = true;
					hints.add("Possible annotation: "
							+ "@ForwardedFieldsSecond(\"" + ff2 + "\")");
				}
			} else {
				final String ff = returnValue.toForwardedFieldsExpression(Input.INPUT_1);
				if (ff != null && ff.length() > 0) {
					added = true;
					hints.add("Possible annotation: "
							+ "@ForwardedFields(\"" + ff + "\")");
				}
			}
		}
		if (!added) {
			hints.add("Possible annotations: none.");
		}
	}

	public void printToLogger(Logger log) {
		StringBuilder sb = new StringBuilder();
		sb.append("Code analysis result for '" + externalUdfName + " (" + udfClass.getName() + ")':");
		sb.append("\nNumber of object creations: "
				+ newOperationCounterTopLevel + " in method / " + newOperationCounterOverall + " transitively");

		for (String hint : hints) {
			sb.append('\n');
			sb.append(hint);
		}

		if (warning) {
			log.warn(sb.toString());
		}
		else {
			log.info(sb.toString());
		}
	}

	public TaggedValue getInput1AsTaggedValue() {
		final int[] groupedKeys;
		if (keys1 != null) {
			groupedKeys = keys1.computeLogicalKeyPositions();
		}
		else {
			groupedKeys = null;
		}
		final TaggedValue input1 = convertTypeInfoToTaggedValue(Input.INPUT_1, in1Type, "", null, groupedKeys);
		// store the input and a copy of it to check for modification afterwards
		if (isFilterFunction) {
			filterInputRef = input1;
			filterInputCopy = input1.copy();
		}
		return input1;
	}

	public TaggedValue getInput2AsTaggedValue() {
		final int[] groupedKeys;
		if (keys2 != null) {
			groupedKeys = keys2.computeLogicalKeyPositions();
		}
		else {
			groupedKeys = null;
		}
		return convertTypeInfoToTaggedValue(Input.INPUT_2, in2Type, "", null, groupedKeys);
	}

	private void addHintOrThrowException(String msg) {
		if (throwErrorExceptions) {
			throw new CodeErrorException(externalUdfName + ": " + msg);
		}
		else {
			warning = true;
			hints.add(msg);
		}
	}

	public void handleNullReturn() {
		addHintOrThrowException("Function returns 'null' values. This can lead to errors during runtime.");
	}

	public void handlePutStatic() {
		addHintOrThrowException("Function modifies static fields. This can lead to unexpected behaviour during runtime.");
	}

	public void handleInvalidTupleAccess() {
		addHintOrThrowException("Function contains tuple accesses with invalid indexes. This can lead to errors during runtime.");
	}
}
