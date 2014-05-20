/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.java.functions;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.SemanticProperties;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFieldsSecond;
import eu.stratosphere.types.TypeInformation;

public class SemanticPropUtil {

	private final static String REGEX_LIST = "(\\s*(\\d+\\s*,\\s*)*(\\d+\\s*))";
	private final static String REGEX_FORWARD = "(\\s*(\\d+)\\s*->(" + REGEX_LIST + "|(\\*)))";
	private final static String REGEX_LIST_OR_FORWARD = "(" + REGEX_LIST + "|" + REGEX_FORWARD + ")";
	private final static String REGEX_ANNOTATION = "(\\s*(" + REGEX_LIST_OR_FORWARD + "\\s*;\\s*)*(" + REGEX_LIST_OR_FORWARD + "\\s*))";

	private static final Pattern PATTERN_FORWARD = Pattern.compile(REGEX_FORWARD);
	private static final Pattern PATTERN_ANNOTATION = Pattern.compile(REGEX_ANNOTATION);
	private static final Pattern PATTERN_LIST = Pattern.compile(REGEX_LIST);

	private static final Pattern PATTERN_DIGIT = Pattern.compile("\\d+");

	public static SingleInputSemanticProperties getSemanticPropsSingle(Set<Annotation> set, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (set == null) {
			return null;
		}
		Iterator<Annotation> it = set.iterator();
		SingleInputSemanticProperties result = null;

		//non tuple types are not yet supported for annotations
		if (!inType.isTupleType() || !outType.isTupleType()) {
			return null;
		}

		while (it.hasNext()) {
			if (result == null) {
				result = new SingleInputSemanticProperties();
			}

			Annotation ann = it.next();

			if (ann instanceof ConstantFields) {
				ConstantFields cf = (ConstantFields) ann;
				parseConstantFields(cf.value(), result, inType, outType);
			} else if (ann instanceof ConstantFieldsExcept) {
				ConstantFieldsExcept cfe = (ConstantFieldsExcept) ann;
				parseConstantFieldsExcept(cfe.value(), result, inType, outType);
			} else if (ann instanceof ReadFields) {
				ReadFields rf = (ReadFields) ann;
				parseReadFields(rf.value(), result, inType, outType);
			}
		}
		return result;
	}

	private static void parseConstantFields(String[] cf, SingleInputSemanticProperties sm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (cf == null) {
			return;
		}
		for (String s : cf) {
			if (s != null) {
				readConstantSet(sm, s, inType, outType, 0);
			}
		}
	}

	private static void readConstantSet(SemanticProperties sp, String s, TypeInformation<?> inType, TypeInformation<?> outType, int input) {
		if (s.equals("*")) {
			if (sp instanceof SingleInputSemanticProperties) {
				for (int i = 0; i < inType.getArity() && i < outType.getArity(); i++) {
					((SingleInputSemanticProperties) sp).addForwardedField(i, i);
				}
			} else if (sp instanceof DualInputSemanticProperties) {
				for (int i = 0; i < inType.getArity() && i < outType.getArity(); i++) {
					if (input == 0) {
						((DualInputSemanticProperties) sp).addForwardedField1(i, i);
					} else if (input == 1) {
						((DualInputSemanticProperties) sp).addForwardedField2(i, i);
					}
				}
			}
			return;
		}

		Matcher matcher = PATTERN_ANNOTATION.matcher(s);

		if (!matcher.matches()) {
			throw new RuntimeException("Unrecognized annotation string format.");
		}

		Matcher forwardMatcher = PATTERN_FORWARD.matcher(s);
		while (forwardMatcher.find()) {
			int sourceField = Integer.valueOf(forwardMatcher.group(2));
			if (!isValidField(inType, sourceField)) {
				throw new IndexOutOfBoundsException("Annotation: Field " + sourceField + " not available in the input tuple.");
			}

			if (forwardMatcher.group(7) != null) {
				if (sp instanceof SingleInputSemanticProperties) {
					for (int i = 0; i < outType.getArity(); i++) {
						((SingleInputSemanticProperties) sp).addForwardedField(sourceField, i);
					}
				} else if (sp instanceof DualInputSemanticProperties) {
					for (int i = 0; i < outType.getArity(); i++) {
						if (input == 0) {
							((DualInputSemanticProperties) sp).addForwardedField1(sourceField, i);
						} else if (input == 1) {
							((DualInputSemanticProperties) sp).addForwardedField2(sourceField, i);
						}
					}
				}
				continue;
			}
			String found = forwardMatcher.group(4);
			FieldSet fs = readFieldSetFromString(found, inType, outType);

			if (sp instanceof SingleInputSemanticProperties) {
				((SingleInputSemanticProperties) sp).addForwardedField(sourceField, fs);
			} else if (sp instanceof DualInputSemanticProperties) {
				if (input == 0) {
					((DualInputSemanticProperties) sp).addForwardedField1(sourceField, fs);
				} else if (input == 1) {
					((DualInputSemanticProperties) sp).addForwardedField2(sourceField, fs);
				}
			}
		}
		s = forwardMatcher.replaceAll("");

		Matcher listMatcher = PATTERN_LIST.matcher(s);

		while (listMatcher.find()) {
			String list = listMatcher.group();
			FieldSet fs = readFieldSetFromString(list, inType, outType);
			for (int i : fs) {
				if (sp instanceof SingleInputSemanticProperties) {
					((SingleInputSemanticProperties) sp).addForwardedField(i, i);
				} else if (sp instanceof DualInputSemanticProperties) {
					if (input == 0) {
						((DualInputSemanticProperties) sp).addForwardedField1(i, i);
					} else if (input == 1) {
						((DualInputSemanticProperties) sp).addForwardedField2(i, i);
					}
				}
			}
		}
	}

	private static void parseConstantFieldsFirst(String[] cff, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (cff == null) {
			return;
		}

		for (String s : cff) {
			if (s != null) {
				readConstantSet(dm, s, inType, outType, 0);
			}
		}
	}

	private static void parseConstantFieldsSecond(String[] cfs, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (cfs == null) {
			return;
		}

		for (String s : cfs) {
			if (s != null) {
				readConstantSet(dm, s, inType, outType, 1);
			}
		}
	}

	private static void parseConstantFieldsFirstExcept(String[] cffe, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (cffe == null) {
			return;
		}

		for (String str : cffe) {
			if (str == null) {
				continue;
			}

			FieldSet fs = readFieldSetFromString(str, inType, outType);

			for (int i = 0; i < outType.getArity(); i++) {
				if (!fs.contains(i)) {
					dm.addForwardedField1(i, i);
				}
			}
		}
	}

	private static void parseConstantFieldsSecondExcept(String[] cfse, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (cfse == null) {
			return;
		}

		for (String str : cfse) {
			if (str == null) {
				continue;
			}

			FieldSet fs = readFieldSetFromString(str, inType, outType);

			for (int i = 0; i < outType.getArity(); i++) {
				if (!fs.contains(i)) {
					dm.addForwardedField2(i, i);
				}
			}
		}
	}

	private static void parseReadFieldsFirst(String[] rf, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (rf == null) {
			return;
		}

		for (String str : rf) {
			if (str != null) {
				FieldSet fs = readFieldSetFromString(str, inType, outType);
				dm.addReadFields1(fs);
			}
		}
	}

	private static void parseReadFieldsSecond(String[] rf, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (rf == null) {
			return;
		}

		for (String str : rf) {
			if (str != null) {
				FieldSet fs = readFieldSetFromString(str, inType, outType);
				dm.addReadFields2(fs);
			}
		}
	}


	private static boolean isValidField(TypeInformation<?> type, int field) {
		return field >= 0 && field < type.getArity();
	}

	private static void parseConstantFieldsExcept(String[] cfe, SingleInputSemanticProperties sm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (cfe == null) {
			return;
		}

		for (String str : cfe) {
			if (str != null) {
				FieldSet fs = readFieldSetFromString(str, inType, outType);

				for (int i = 0; i < outType.getArity(); i++) {
					if (!fs.contains(i)) {
						sm.addForwardedField(i, i);
					}
				}
			}
		}
	}

	private static FieldSet readFieldSetFromString(String s, TypeInformation<?> inType, TypeInformation<?> outType) {
		Matcher matcher = PATTERN_LIST.matcher(s);

		if (!matcher.matches()) {
			throw new RuntimeException("Unrecognized annotation string format.");
		}

		matcher = PATTERN_DIGIT.matcher(s);
		FieldSet fs = new FieldSet();

		while (matcher.find()) {
			int field = Integer.valueOf(matcher.group());
			if (!isValidField(outType, field) || !isValidField(inType, field)) {
				throw new IndexOutOfBoundsException("Annotation: Field " + field + " not available in the output tuple.");
			}
			fs.add(field);
		}
		return fs;
	}

	private static void parseReadFields(String[] rf, SingleInputSemanticProperties sm, TypeInformation<?> inType, TypeInformation<?> outType) {
		if (rf == null) {
			return;
		}

		for (String str : rf) {
			if (str != null) {
				FieldSet fs = readFieldSetFromString(str, inType, outType);
				sm.addReadFields(fs);
			}
		}
	}

	public static SingleInputSemanticProperties getSemanticPropsSingleFromString(String[] constantSet, String[] constantSetExcept, String[] readSet, TypeInformation<?> inType, TypeInformation<?> outType) {
		SingleInputSemanticProperties result = new SingleInputSemanticProperties();
		parseConstantFields(constantSet, result, inType, outType);
		parseConstantFieldsExcept(constantSetExcept, result, inType, outType);
		parseReadFields(readSet, result, inType, outType);
		return result;
	}

	public static DualInputSemanticProperties getSemanticPropsDualFromString(String[] constantSetFirst, String[] constantSetSecond, String[] constantSetFirstExcept,
							String[] constantSetSecondExcept, String[] readFieldsFirst, String[] readFieldsSecond, TypeInformation<?> inType1, TypeInformation<?> inType2, TypeInformation<?> outType) {
		DualInputSemanticProperties result = new DualInputSemanticProperties();
		parseConstantFieldsFirst(constantSetFirst, result, inType1, outType);
		parseConstantFieldsSecond(constantSetSecond, result, inType2, outType);
		parseConstantFieldsFirstExcept(constantSetFirstExcept, result, inType1, outType);
		parseConstantFieldsSecondExcept(constantSetSecondExcept, result, inType2, outType);
		parseReadFieldsFirst(readFieldsFirst, result, inType1, outType);
		parseReadFieldsSecond(readFieldsSecond, result, inType2, outType);
		return result;
	}

	public static DualInputSemanticProperties getSemanticPropsDual(Set<Annotation> set, TypeInformation<?> inType1, TypeInformation<?> inType2, TypeInformation<?> outType) {
		if (set == null) {
			return null;
		}

		Iterator<Annotation> it = set.iterator();
		DualInputSemanticProperties result = null;

		//non tuple types are not yet supported for annotations
		if (!inType1.isTupleType() || !inType2.isTupleType() || !outType.isTupleType()) {
			return null;
		}

		while (it.hasNext()) {
			if (result == null) {
				result = new DualInputSemanticProperties();
			}

			Annotation ann = it.next();

			if (ann instanceof ConstantFieldsFirst) {
				ConstantFieldsFirst cff = (ConstantFieldsFirst) ann;
				parseConstantFieldsFirst(cff.value(), result, inType1, outType);
			} else if (ann instanceof ConstantFieldsSecond) {
				ConstantFieldsSecond cfs = (ConstantFieldsSecond) ann;
				parseConstantFieldsSecond(cfs.value(), result, inType2, outType);
			} else if (ann instanceof ConstantFieldsFirstExcept) {
				ConstantFieldsFirstExcept cffe = (ConstantFieldsFirstExcept) ann;
				parseConstantFieldsFirstExcept(cffe.value(), result, inType1, outType);
			} else if (ann instanceof ConstantFieldsSecondExcept) {
				ConstantFieldsSecondExcept cfse = (ConstantFieldsSecondExcept) ann;
				parseConstantFieldsSecondExcept(cfse.value(), result, inType2, outType);
			} else if (ann instanceof ReadFieldsFirst) {
				ReadFieldsFirst rff = (ReadFieldsFirst) ann;
				parseReadFieldsFirst(rff.value(), result, inType1, outType);
			} else if (ann instanceof ReadFieldsSecond) {
				ReadFieldsSecond rfs = (ReadFieldsSecond) ann;
				parseReadFieldsSecond(rfs.value(), result, inType2, outType);
			}
		}
		return result;
	}
}
