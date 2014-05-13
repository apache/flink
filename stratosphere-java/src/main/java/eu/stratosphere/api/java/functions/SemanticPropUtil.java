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
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFieldsSecond;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ReadFields;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class SemanticPropUtil {

    private final static String REGEX_ANNOTATION = "\\s*(\\d+)\\s*->(\\s*(\\d+\\s*,\\s*)*(\\d+\\s*))";

    public static SingleInputSemanticProperties getSemanticPropsSingle(Set<Annotation> set, TypeInformation<?> inType, TypeInformation<?> outType) {
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
       for (String s: cf) {
            readConstantSet(sm, s, inType, outType, 0);
       }
    }

    private static void readConstantSet(SemanticProperties sp, String s, TypeInformation<?> inType, TypeInformation<?> outType, int input) {
        Pattern check = Pattern.compile(REGEX_ANNOTATION);
        Matcher matcher = check.matcher(s);
        int sourceField = 0;

        if (!matcher.matches()) {
            throw new RuntimeException("Wrong annotation String format. Please read the documentation.");
        }

        sourceField = Integer.valueOf(matcher.group(1));
        if (!isValidField(inType, sourceField)) {
            throw new IndexOutOfBoundsException("Annotation: Field " + sourceField + " not available in the input tuple.");
        }
        FieldSet fs = readFieldSetFromString(matcher.group(2), inType, outType);

        if (sp instanceof SingleInputSemanticProperties) {
            ((SingleInputSemanticProperties) sp).addForwardedField(sourceField, fs);
        } else if (sp instanceof  DualInputSemanticProperties) {
            if (input == 0) {
                ((DualInputSemanticProperties) sp).addForwardedField1(sourceField, fs);
            } else if (input == 1) {
                ((DualInputSemanticProperties) sp).addForwardedField2(sourceField, fs);
            }
        }
    }

    private static void parseConstantFieldsFirst(String[] cff,  DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
        Pattern check = Pattern.compile(REGEX_ANNOTATION);
        for (String s: cff) {
            readConstantSet(dm, s, inType, outType, 0);
        }
    }

    private static void parseConstantFieldsSecond(String[] cfs,  DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
        Pattern check = Pattern.compile(REGEX_ANNOTATION);
        for (String s: cfs) {
            readConstantSet(dm, s, inType, outType, 1);
        }
    }

    private static void parseConstantFieldsFirstExcept(String cffe, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
        FieldSet fs = readFieldSetFromString(cffe, inType, outType);

        for (int i = 0; i < outType.getArity(); i++) {
            if (!fs.contains(i)) {
                dm.addForwardedField1(i, i);
            }
        }
    }

    private static void parseConstantFieldsSecondExcept(String cfse, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
        FieldSet fs = readFieldSetFromString(cfse, inType, outType);

        for (int i = 0; i < outType.getArity(); i++) {
            if (!fs.contains(i)) {
                dm.addForwardedField2(i, i);
            }
        }
    }

    private static void parseReadFieldsFirst(String rf, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
        FieldSet fs = readFieldSetFromString(rf, inType, outType);
        dm.addReadFields1(fs);
    }

    private static void parseReadFieldsSecond(String rf, DualInputSemanticProperties dm, TypeInformation<?> inType, TypeInformation<?> outType) {
        FieldSet fs = readFieldSetFromString(rf, inType, outType);
        dm.addReadFields2(fs);
    }


    private static boolean isValidField(TypeInformation<?> type, int field) {
        if (field > type.getArity() || field < 0) {
            return false;
        }
        return true;
    }

	private static void parseConstantFieldsExcept(String cfe, SingleInputSemanticProperties sm, TypeInformation<?> inType, TypeInformation<?> outType) {
        FieldSet fs = readFieldSetFromString(cfe, inType, outType);

        for (int i = 0; i < outType.getArity(); i++) {
            if (!fs.contains(i)) {
                sm.addForwardedField(i,i);
            }
        }
    }

    private static FieldSet readFieldSetFromString(String s, TypeInformation<?> inType, TypeInformation<?> outType) {
        Pattern check = Pattern.compile("\\s*(\\d+\\s*,\\s*)*(\\d+\\s*)");
        Pattern digit = Pattern.compile("\\d+");

        Matcher matcher = check.matcher(s);

        if (!matcher.matches()) {
            throw new RuntimeException("Wrong annotation String format. Please read the documentation.");
        }

        matcher = digit.matcher(s);
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

	private static void parseReadFields(String rf, SingleInputSemanticProperties sm, TypeInformation<?> inType, TypeInformation<?> outType) {
        FieldSet fs = readFieldSetFromString(rf, inType, outType);
        sm.addReadFields(fs);
	}

    public static SingleInputSemanticProperties getSemanticPropsSingleFromString(String[] ConstantSet, String constantSetExcept, String ReadSet, TypeInformation<?> inType, TypeInformation<?> outType) {
        return null;
    }

    public static DualInputSemanticProperties getSemanticPropsDualFromString(String[] constantSetFirst, String[] constantSetSecond, String constantSetFirstExcept,
                                                                             String constantSetSecondExcept, String readFieldsFirst, String readFieldsSecond, TypeInformation<?> inType1, TypeInformation<?> inType2, TypeInformation<?> outType) {
        return null;
    }

	public static DualInputSemanticProperties getSemanticPropsDual(Set<Annotation> set, TypeInformation<?> inType1, TypeInformation<?> inType2, TypeInformation<?> outType) {
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
            } else  if (ann instanceof ConstantFieldsSecond) {
                ConstantFieldsSecond cfs = (ConstantFieldsSecond) ann;
                parseConstantFieldsSecond(cfs.value(), result, inType2, outType);
            } else if (ann instanceof ConstantFieldsFirstExcept) {
                ConstantFieldsFirstExcept cffe = (ConstantFieldsFirstExcept) ann;
                parseConstantFieldsFirstExcept(cffe.value(), result, inType1, outType);
            }  else if (ann instanceof ConstantFieldsSecondExcept) {
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
