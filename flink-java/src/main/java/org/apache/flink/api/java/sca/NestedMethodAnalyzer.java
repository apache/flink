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
import org.apache.flink.api.java.sca.TaggedValue.Tag;

import org.apache.flink.shaded.asm5.org.objectweb.asm.Type;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.AbstractInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.FieldInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.IntInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.LdcInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.MethodInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.MethodNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.TypeInsnNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.AnalyzerException;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.BasicInterpreter;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.BasicValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.findMethodNode;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.hasImportantDependencies;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.isTagged;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.mergeReturnValues;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.removeUngroupedInputs;
import static org.apache.flink.api.java.sca.UdfAnalyzerUtils.tagged;

/**
 * Extends ASM's BasicInterpreter. Instead of ASM's BasicValues, it introduces
 * TaggedValues which extend BasicValue and allows for appending interesting
 * information to values. The NestedMethodAnalyzer follows interesting values
 * and analyzes nested methods (control/method flow) if necessary.
 */
@Internal
public class NestedMethodAnalyzer extends BasicInterpreter {

	// reference to current UDF analysis context
	private final UdfAnalyzer analyzer;

	// flag that indicates if current method is still the Function's SAM method
	private final boolean topLevelMethod;

	// method description and arguments
	private final String owner;
	private final MethodNode methodNode;
	private final List<BasicValue> argumentValues;

	// remaining nesting level
	private final int remainingNesting;

	// ASM analyzer which analyzes this methodNode
	private ModifiedASMAnalyzer modifiedAsmAnalyzer;

	public NestedMethodAnalyzer(UdfAnalyzer analyzer, String owner, MethodNode methodNode,
			List<BasicValue> argumentValues, int remainingNesting, boolean topLevelMethod) {
		this.analyzer = analyzer;
		this.topLevelMethod = topLevelMethod;
		this.owner = owner;
		this.methodNode = methodNode;
		this.argumentValues = argumentValues;

		this.remainingNesting = remainingNesting;
		if (remainingNesting < 0) {
			throw new CodeAnalyzerException("Maximum nesting level reached.");
		}
	}

	public TaggedValue analyze() throws AnalyzerException {
		modifiedAsmAnalyzer = new ModifiedASMAnalyzer(this);

		// FOR DEBUGGING
		//		final Printer printer = new Textifier();
		//		final TraceMethodVisitor mp = new TraceMethodVisitor(printer);
		//		System.out.println(methodNode.name + " " + methodNode.desc);
		//		Iterator<AbstractInsnNode> it = methodNode.instructions.iterator();
		//		while (it.hasNext()) {
		//			it.next().accept(mp);
		//			StringWriter sw = new StringWriter();
		//			printer.print(new PrintWriter(sw));
		//			printer.getText().clear();
		//			System.out.println(sw.toString());
		//		}
		modifiedAsmAnalyzer.analyze(owner, methodNode);
		return mergeReturnValues(returnValues);
	}

	@SuppressWarnings("unchecked")
	private TaggedValue invokeNestedMethod(List<? extends BasicValue> values,
			final MethodInsnNode methodInsn) throws AnalyzerException {
		final Object[] mn = findMethodNode(methodInsn.owner, methodInsn.name, methodInsn.desc);
		MethodNode methodNode = (MethodNode) mn[0];
		// recursion
		if (methodNode.name.equals(this.methodNode.name)
				&& methodNode.desc.equals(this.methodNode.desc)) {
			// TODO recursion are not supported perfectly yet
			// recursion only work if the nested call follows at least one return statement
			// return the values that are present so far
			return mergeReturnValues(returnValues);
		}
		final NestedMethodAnalyzer nma = new NestedMethodAnalyzer(analyzer, (String) mn[1],
				(MethodNode) mn[0],
				(List<BasicValue>) values, remainingNesting - 1,
				topLevelMethod && isBridgeMethod());
		return nma.analyze();
	}

	private boolean isBridgeMethod() {
		return (methodNode.access & ACC_BRIDGE) == ACC_BRIDGE;
	}

	private boolean isGetRuntimeContext(MethodInsnNode methodInsnNode) {
		return methodInsnNode.name.equals("getRuntimeContext")
				&& findMethodNode(methodInsnNode.owner, methodInsnNode.name, methodInsnNode.desc)[1]
				.equals("org/apache/flink/api/common/functions/AbstractRichFunction");
	}

	private Type checkForUnboxing(String name, String methodOwner) {
		// for performance improvement
		if (!methodOwner.startsWith("java/lang/")
				|| !name.endsWith("Value")) {
			return null;
		}

		final String actualType = methodOwner.substring(10);
		final String convertedType = name.substring(0, name.length() - 5);

		if (convertedType.equals("byte") && actualType.equals("Byte")) {
			return Type.BYTE_TYPE;
		}
		else if (convertedType.equals("short") && actualType.equals("Short")) {
			return Type.SHORT_TYPE;
		}
		else if (convertedType.equals("int") && actualType.equals("Integer")) {
			return Type.INT_TYPE;
		}
		else if (convertedType.equals("long") && actualType.equals("Long")) {
			return Type.LONG_TYPE;
		}
		else if (convertedType.equals("boolean") && actualType.equals("Boolean")) {
			return Type.BOOLEAN_TYPE;
		}
		else if (convertedType.equals("char") && actualType.equals("Character")) {
			return Type.CHAR_TYPE;
		}
		else if (convertedType.equals("float") && actualType.equals("Float")) {
			return Type.FLOAT_TYPE;
		}
		else if (convertedType.equals("double") && actualType.equals("Double")) {
			return Type.DOUBLE_TYPE;
		}
		return null;
	}

	private Type checkForBoxing(String name, String desc, String methodOwner) {
		// for performance improvement
		if (!methodOwner.startsWith("java/lang/")
				|| !name.equals("valueOf")) {
			return null;
		}

		final String convertedType = methodOwner.substring(10);

		if (convertedType.equals("Byte") && desc.equals("(B)Ljava/lang/Byte;")) {
			return Type.BYTE_TYPE;
		}
		else if (convertedType.equals("Short") && desc.equals("(S)Ljava/lang/Short;")) {
			return Type.SHORT_TYPE;
		}
		else if (convertedType.equals("Integer") && desc.equals("(I)Ljava/lang/Integer;")) {
			return Type.INT_TYPE;
		}
		else if (convertedType.equals("Long") && desc.equals("(J)Ljava/lang/Long;")) {
			return Type.LONG_TYPE;
		}
		else if (convertedType.equals("Boolean") && desc.equals("(Z)Ljava/lang/Boolean;")) {
			return Type.BOOLEAN_TYPE;
		}
		else if (convertedType.equals("Character") && desc.equals("(C)Ljava/lang/Character;")) {
			return Type.CHAR_TYPE;
		}
		else if (convertedType.equals("Float") && desc.equals("(F)Ljava/lang/Float;")) {
			return Type.FLOAT_TYPE;
		}
		else if (convertedType.equals("Double") && desc.equals("(D)Ljava/lang/Double;")) {
			return Type.DOUBLE_TYPE;
		}
		return null;
	}

	// --------------------------------------------------------------------------------------------
	// Interpreter
	// --------------------------------------------------------------------------------------------

	// variable that maps the method's arguments to ASM values
	private int curArgIndex = -1;
	private List<TaggedValue> returnValues = new ArrayList<TaggedValue>();

	// see ModifiedASMFrame
	ModifiedASMFrame currentFrame;
	boolean rightMergePriority;

	@Override
	public BasicValue newValue(Type type) {
		TaggedValue tv;
		switch (analyzer.getState()) {
			// skip "return"
			case UdfAnalyzer.STATE_CAPTURE_RETURN:
				analyzer.setState(UdfAnalyzer.STATE_CAPTURE_THIS);
				curArgIndex++;
				return super.newValue(type);
			// tag "this"
			case UdfAnalyzer.STATE_CAPTURE_THIS:
				analyzer.setState(UdfAnalyzer.STATE_CAPTURE_INPUT1);
				tv = new TaggedValue(type, Tag.THIS);
				curArgIndex++;
				return tv;
			// tag input 1
			case UdfAnalyzer.STATE_CAPTURE_INPUT1:
				if (analyzer.isUdfBinary() || analyzer.isUdfReduceFunction()) {
					analyzer.setState(UdfAnalyzer.STATE_CAPTURE_INPUT2);
				} else if (analyzer.hasUdfCollector()) {
					analyzer.setState(UdfAnalyzer.STATE_CAPTURE_COLLECTOR);
				} else {
					analyzer.setState(UdfAnalyzer.STATE_END_OF_CAPTURING);
				}

				// input is iterable
				if (analyzer.hasUdfIterableInput()) {
					tv = new TaggedValue(type, Tag.INPUT_1_ITERABLE);
				}
				else {
					tv = analyzer.getInput1AsTaggedValue();
				}
				curArgIndex++;
				return tv;
			// tag input 2
			case UdfAnalyzer.STATE_CAPTURE_INPUT2:
				if (analyzer.hasUdfCollector()) {
					analyzer.setState(UdfAnalyzer.STATE_CAPTURE_COLLECTOR);
				} else {
					analyzer.setState(UdfAnalyzer.STATE_END_OF_CAPTURING);
				}

				// input is iterable
				if (analyzer.hasUdfIterableInput()) {
					tv = new TaggedValue(type, Tag.INPUT_2_ITERABLE);
				}
				// special case: reduce function
				else if (analyzer.isUdfReduceFunction()) {
					tv = analyzer.getInput1AsTaggedValue();
				}
				else {
					tv = analyzer.getInput2AsTaggedValue();
				}
				curArgIndex++;
				return tv;
			// tag collector
			case UdfAnalyzer.STATE_CAPTURE_COLLECTOR:
				analyzer.setState(UdfAnalyzer.STATE_END_OF_CAPTURING);
				tv = new TaggedValue(type, Tag.COLLECTOR);
				curArgIndex++;
				return tv;
			// if capturing has finished do not tag the arguments any more
			// but copy values where necessary (for nested methods)
			case UdfAnalyzer.STATE_END_OF_CAPTURING:
			default:
				// skip return type
				if (curArgIndex < 0) {
					curArgIndex++;
				}
				// return method's arguments
				else if (argumentValues != null && curArgIndex < argumentValues.size()) {
					return argumentValues.get(curArgIndex++);
				}
				return super.newValue(type);
		}
	}

	@Override
	public BasicValue newOperation(AbstractInsnNode insn) throws AnalyzerException {
		switch (insn.getOpcode()) {
			case ACONST_NULL:
				return new TaggedValue(Type.getObjectType("null"), Tag.NULL);
			case NEW:
				analyzer.incrNewOperationCounters(topLevelMethod);
				// make new objects a tagged value to have possibility to tag an
				// input container later
				return new TaggedValue(Type.getObjectType(((TypeInsnNode) insn).desc));
			// tag "int"-like constants
			case BIPUSH:
			case SIPUSH:
				final IntInsnNode intInsn = (IntInsnNode) insn;
				return new TaggedValue(intInsn.operand);
			case LDC:
				final Object cst = ((LdcInsnNode) insn).cst;
				if (cst instanceof Integer) {
					return new TaggedValue((Integer) cst);
				}
				return super.newOperation(insn);
			case ICONST_M1:
				return new TaggedValue(-1);
			case ICONST_0:
				return new TaggedValue(0);
			case ICONST_1:
				return new TaggedValue(1);
			case ICONST_2:
				return new TaggedValue(2);
			case ICONST_3:
				return new TaggedValue(3);
			case ICONST_4:
				return new TaggedValue(4);
			case ICONST_5:
				return new TaggedValue(5);
			default:
				return super.newOperation(insn);
		}
	}

	@Override
	public BasicValue copyOperation(AbstractInsnNode insn, BasicValue value) throws AnalyzerException {
		switch (insn.getOpcode()) {
			case ILOAD:
				// int constants are only supported if they are used in the subsequent operation
				// otherwise we can not guarantee that it isn't modified elsewhere (e.g. do-while loop)
				if (isTagged(value) && tagged(value).isIntConstant()) {
					tagged(value).makeRegular();
				}
				return super.copyOperation(insn, value);
			// if input is stored "call by value", it will be copied
			case ISTORE:
			case LSTORE:
			case FSTORE:
			case DSTORE:
			case ASTORE:
			case DUP:
			case DUP_X1:
			case DUP_X2:
			case DUP2:
			case DUP2_X1:
			case DUP2_X2:
				if (isTagged(value) && tagged(value).isInput() && tagged(value).isCallByValue()) {
					return tagged(value).copy();
				} else {
					return super.copyOperation(insn, value);
				}
			default:
				return super.copyOperation(insn, value);
		}
	}

	@Override
	public BasicValue unaryOperation(AbstractInsnNode insn, BasicValue value)
			throws AnalyzerException {
		switch (insn.getOpcode()) {
			// modify jump instructions if we can assume that the hasNext operation will always
			// be true at the first call
			case IFEQ:
				if (isTagged(value) && tagged(value).isIteratorTrueAssumption()) {
					modifiedAsmAnalyzer.requestIFEQLoopModification();
				}
				return super.unaryOperation(insn, value);
			case IFNE:
				if (isTagged(value) && tagged(value).isIteratorTrueAssumption()) {
					modifiedAsmAnalyzer.requestIFNELoopModification();
				}
				return super.unaryOperation(insn, value);

			case CHECKCAST:
				return value;
			case PUTSTATIC:
				analyzer.handlePutStatic();
				return super.unaryOperation(insn, value);
			case GETFIELD:
				final FieldInsnNode field = (FieldInsnNode) insn;
				// skip untagged values
				if (!isTagged(value)) {
					return super.unaryOperation(insn, value);
				}
				final TaggedValue taggedValue = (TaggedValue) value;

				// inputs are atomic, a GETFIELD results in undefined state
				if (taggedValue.isInput()) {
					return super.unaryOperation(insn, value);
				}
				// access of input container field
				// or access of a KNOWN UDF instance variable
				else if (taggedValue.canContainFields()
						&& taggedValue.containerContains(field.name)) {
					final TaggedValue tv = taggedValue.getContainerMapping().get(field.name);
					if (tv != null) {
						return tv;
					}
				}
				// access of a yet UNKNOWN UDF instance variable
				else if (taggedValue.isThis()
						&& !taggedValue.containerContains(field.name)) {
					final TaggedValue tv = new TaggedValue(Type.getType(field.desc));
					taggedValue.addContainerMapping(field.name, tv, currentFrame);
					return tv;
				}
				// access of a yet unknown container, mark it as a container
				else if (taggedValue.isRegular()) {
					taggedValue.setTag(Tag.CONTAINER);
					final TaggedValue tv = new TaggedValue(Type.getType(field.desc));
					taggedValue.addContainerMapping(field.name, tv, currentFrame);
					return tv;
				}
				return super.unaryOperation(insn, value);
			case IINC:
					// modification of a local variable or input
					if (isTagged(value) && (tagged(value).isIntConstant() || tagged(value).isInput())) {
						tagged(value).makeRegular();
					}
					return super.unaryOperation(insn, value);
			default:
				return super.unaryOperation(insn, value);
		}
	}

	@Override
	public BasicValue ternaryOperation(AbstractInsnNode insn, BasicValue value1, BasicValue value2,
			BasicValue value3) throws AnalyzerException {
		// if array is an input, make it regular since the input is modified
		if (isTagged(value1) && tagged(value1).isInput()) {
			tagged(value1).makeRegular();
		}
		return super.ternaryOperation(insn, value1, value2, value3);
	}

	@Override
	public BasicValue binaryOperation(AbstractInsnNode insn, BasicValue value1,
			BasicValue value2) throws AnalyzerException {
		switch (insn.getOpcode()) {
			case PUTFIELD: // put value2 into value1
				// skip untagged values
				if (!isTagged(value1)) {
					return null;
				}

				final TaggedValue taggedValue = (TaggedValue) value1;
				final FieldInsnNode field = (FieldInsnNode) insn;
				final boolean value2HasInputDependency = hasImportantDependencies(value2);

				// if value1 is not an input, make value1 a container and add value2 to it
				// PUTFIELD on inputs is not allowed
				if (!taggedValue.isInput() && value2HasInputDependency) {
					if (!taggedValue.canContainFields()) {
						taggedValue.setTag(Tag.CONTAINER);
					}
					taggedValue.addContainerMapping(field.name, tagged(value2), currentFrame);
				}
				// if value1 is filled with non-input, make it container and mark the field
				// PUTFIELD on inputs is not allowed
				else if (!taggedValue.isInput() && !value2HasInputDependency) {
					if (!taggedValue.canContainFields()) {
						taggedValue.setTag(Tag.CONTAINER);
					}
					taggedValue.addContainerMapping(field.name, null, currentFrame);
				}
				// PUTFIELD on input leads to input modification
				// make input regular
				else if (taggedValue.isInput()) {
					taggedValue.makeRegular();
				}
				return null;
			default:
				return super.binaryOperation(insn, value1, value2);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public BasicValue naryOperation(AbstractInsnNode insn, List rawValues) throws AnalyzerException {
		final List<BasicValue> values = (List<BasicValue>) rawValues;
		boolean isStatic;
		switch (insn.getOpcode()) {
			case INVOKESTATIC:
			case INVOKESPECIAL:
			case INVOKEVIRTUAL:
			case INVOKEINTERFACE:
				isStatic = insn.getOpcode() == INVOKESTATIC;
				final MethodInsnNode method = (MethodInsnNode) insn;
				String methodOwner = method.owner;

				// special case: in case that class is extending tuple we need to find the class
				// that contains the actual implementation to determine the tuple size
				if (method.name.equals("getField") || method.name.equals("setField")) {
					try {
						final String newMethodOwner = (String) findMethodNode(methodOwner, method.name, method.desc)[1];
						if (newMethodOwner.startsWith("org/apache/flink/api/java/tuple/Tuple")) {
							methodOwner = newMethodOwner;
						}
					}
					catch (IllegalStateException e) {
						// proceed with the known method owner
					}
				}

				// special case: collect method of Collector
				if (method.name.equals("collect")
						&& methodOwner.equals("org/apache/flink/util/Collector")
						&& isTagged(values.get(0))
						&& tagged(values.get(0)).isCollector()) {
					// check for invalid return value
					if (isTagged(values.get(1)) && tagged(values.get(1)).isNull()) {
						analyzer.handleNullReturn();
					}
					// valid return value with input dependencies
					else if (hasImportantDependencies(values.get(1))){
						// add a copy and a reference
						// to capture the current state and future changes in alternative paths
						analyzer.getCollectorValues().add(tagged(values.get(1)));
						analyzer.getCollectorValues().add(tagged(values.get(1)).copy());
					}
					// valid return value without input dependencies
					else {
						analyzer.getCollectorValues().add(null);
					}
				}
				// special case: iterator method of Iterable
				else if (method.name.equals("iterator")
						&& methodOwner.equals("java/lang/Iterable")
						&& isTagged(values.get(0))
						&& tagged(values.get(0)).isInputIterable()) {
					return new TaggedValue(Type.getObjectType("java/util/Iterator"),
							(tagged(values.get(0)).isInput1Iterable()) ? Tag.INPUT_1_ITERATOR : Tag.INPUT_2_ITERATOR);
				}
				// special case: hasNext method of Iterator
				else if (method.name.equals("hasNext")
						&& methodOwner.equals("java/util/Iterator")
						&& isTagged(values.get(0))
						&& tagged(values.get(0)).isInputIterator()
						&& !analyzer.isUdfBinary()
						&& !analyzer.isIteratorTrueAssumptionApplied()) {
					return new TaggedValue(Type.BOOLEAN_TYPE, Tag.ITERATOR_TRUE_ASSUMPTION);
				}
				// special case: next method of Iterator
				else if (method.name.equals("next")
						&& methodOwner.equals("java/util/Iterator")
						&& isTagged(values.get(0))
						&& tagged(values.get(0)).isInputIterator()) {
					// after this call it is not possible to assume "TRUE" of "hasNext()" again
					analyzer.applyIteratorTrueAssumption();
					if (tagged(values.get(0)).isInput1Iterator()) {
						return analyzer.getInput1AsTaggedValue();
					}
					else {
						return analyzer.getInput2AsTaggedValue();
					}
				}
				// if the UDF class contains instance variables that contain input,
				// we need to analyze also methods without input-dependent arguments
				// special case: do not follow the getRuntimeContext method of RichFunctions
				else if (!isStatic
						&& isTagged(values.get(0))
						&& tagged(values.get(0)).isThis()
						&& hasImportantDependencies(values.get(0))
						&& !isGetRuntimeContext(method)) {
					TaggedValue tv = invokeNestedMethod(values, method);
					if (tv != null) {
						return tv;
					}
				}
				// the arguments have input dependencies ("THIS" does not count to the arguments)
				// we can assume that method has at least one argument
				else if ((!isStatic && isTagged(values.get(0)) && tagged(values.get(0)).isThis() && hasImportantDependencies(values, true))
						|| (!isStatic && (!isTagged(values.get(0)) || !tagged(values.get(0)).isThis()) && hasImportantDependencies(values, false))
						|| (isStatic && hasImportantDependencies(values, false))) {
					// special case: Java unboxing/boxing methods on input
					Type newType;
					if (isTagged(values.get(0))
							&& tagged(values.get(0)).isInput()
							&& (!isStatic && (newType = checkForUnboxing(method.name, methodOwner)) != null
							|| (isStatic && (newType = checkForBoxing(method.name, method.desc, methodOwner))
									!= null))) {
						return tagged(values.get(0)).copy(newType);
					}
					// special case: setField method of TupleXX
					else if (method.name.equals("setField")
							&& methodOwner.startsWith("org/apache/flink/api/java/tuple/Tuple")
							&& isTagged(values.get(0))
							) {
						final TaggedValue tuple = tagged(values.get(0));
						tuple.setTag(Tag.CONTAINER);

						// check if fieldPos is constant
						// if not, we can not determine a state for the tuple
						if (!isTagged(values.get(2)) || !tagged(values.get(2)).isIntConstant()) {
							tuple.makeRegular();
						}
						else {
							final int constant = tagged(values.get(2)).getIntConstant();

							if (constant < 0 || Integer.parseInt(methodOwner.split("Tuple")[1]) <= constant) {
								analyzer.handleInvalidTupleAccess();
							}

							// if it is at least tagged, add it anyways
							if (isTagged(values.get(1))) {
								tuple.addContainerMapping("f" + constant, tagged(values.get(1)), currentFrame);
							}
							// mark the field as it has an undefined state
							else {
								tuple.addContainerMapping("f" + constant, null, currentFrame);
							}
						}
					}
					// special case: getField method of TupleXX
					else if (method.name.equals("getField")
							&& methodOwner.startsWith("org/apache/flink/api/java/tuple/Tuple")) {
						final TaggedValue tuple = tagged(values.get(0)); // we can assume that 0 is an input dependent tuple
						// constant field index
						if (isTagged(values.get(1)) // constant
								&& tagged(values.get(1)).isIntConstant()) {
							final int constant = tagged(values.get(1)).getIntConstant();

							if (constant < 0 || Integer.valueOf(methodOwner.split("Tuple")[1]) <= constant) {
								analyzer.handleInvalidTupleAccess();
							}

							if (tuple.containerContains("f" + constant)) {
								final TaggedValue tupleField = tuple.getContainerMapping().get("f" + constant);
								if (tupleField != null) {
									return tupleField;
								}
							}
						}
						// unknown field index
						else {
							// we need to make the tuple regular as we cannot track modifications of fields
							tuple.makeRegular();
							return new TaggedValue(Type.getObjectType("java/lang/Object"));
						}
					}
					// nested method invocation
					else {
						TaggedValue tv = invokeNestedMethod(values, method);
						if (tv != null) {
							return tv;
						}
					}
				}
				return super.naryOperation(insn, values);
			default:
				// TODO support for INVOKEDYNAMIC instructions
				return super.naryOperation(insn, values);
		}
	}

	@Override
	public void returnOperation(AbstractInsnNode insn, BasicValue value,
			BasicValue expected) throws AnalyzerException {
		// return of a null value in the top level UDF method
		if (isTagged(value) && tagged(value).isNull() && topLevelMethod) {
			analyzer.handleNullReturn();
		}
		else if (hasImportantDependencies(value)){
			// add a copy and a reference
			// to capture the current state and future changes in alternative paths
			returnValues.add(tagged(value));
			returnValues.add(tagged(value).copy());
		}
		else {
			returnValues.add(null);
		}
	}

	@Override
	public BasicValue merge(BasicValue v, BasicValue w) {
		// values are not equal
		// BasicValue's equals method is too general
		if ((!isTagged(v) && !w.equals(v)) || (isTagged(v) && !v.equals(w))) {
			// w is a BasicValue
			if (isTagged(v) && !isTagged(w)) {
				return new TaggedValue(w.getType());
			}
			// v is a BasicValue or uninteresting and w is a interesting TaggedValue
			else if ((!isTagged(v) || tagged(v).canNotContainInput())
					&& isTagged(w) && tagged(w).canContainInput()) {
				final TaggedValue taggedW = tagged(w);
				if (hasImportantDependencies(taggedW)
						&& rightMergePriority) {
						 // w has a merge priority, its grouped inputs will be returned
					final TaggedValue returnValue = removeUngroupedInputs(taggedW.copy());
					if (returnValue != null) {
						return returnValue;
					}
					else {
						return new TaggedValue(v.getType());
					}
				}
				return new TaggedValue(v.getType());
			}
			// v is a BasicValue and w is a uninteresting TaggedValue
			else if (!isTagged(v) && isTagged(w) && tagged(w).canNotContainInput()) {
				return v;
			}
			// merge v and w (both TaggedValues), v is interesting
			else if (isTagged(v) && isTagged(w) && tagged(v).canContainInput()) {
				final List<TaggedValue> list = Arrays.asList(tagged(v), tagged(w));
				final TaggedValue returnValue = mergeReturnValues(list);
				if (returnValue != null) {
					return returnValue;
				}
			}
			// v is a TaggedValue and uninteresting
			else if (isTagged(v) && tagged(v).canNotContainInput()) {
				return v;
			}
			// v and w are BasicValues and not equal
			return BasicValue.UNINITIALIZED_VALUE;
		}
		// v and w are equal, return one of them
		return v;
	}
}
