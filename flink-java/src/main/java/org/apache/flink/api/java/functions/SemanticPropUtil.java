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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties.InvalidSemanticAnnotationException;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.functions.FunctionAnnotation.NonForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.NonForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.NonForwardedFieldsSecond;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFieldsSecond;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class that contains helper methods to work with {@link SemanticProperties}. */
@Internal
public final class SemanticPropUtil {

    private static final String REGEX_WILDCARD =
            "[\\"
                    + Keys.ExpressionKeys.SELECT_ALL_CHAR
                    + "\\"
                    + Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA
                    + "]";
    private static final String REGEX_SINGLE_FIELD = "[\\p{L}\\p{Digit}_\\$]+";
    private static final String REGEX_NESTED_FIELDS =
            "(("
                    + REGEX_SINGLE_FIELD
                    + "\\.)*"
                    + REGEX_SINGLE_FIELD
                    + ")(\\."
                    + REGEX_WILDCARD
                    + ")?";

    private static final String REGEX_LIST =
            "((" + REGEX_NESTED_FIELDS + ";)*(" + REGEX_NESTED_FIELDS + ");?)";
    private static final String REGEX_FORWARD =
            "(("
                    + REGEX_NESTED_FIELDS
                    + "|"
                    + REGEX_WILDCARD
                    + ")->("
                    + REGEX_NESTED_FIELDS
                    + "|"
                    + REGEX_WILDCARD
                    + "))";
    private static final String REGEX_FIELD_OR_FORWARD =
            "(" + REGEX_NESTED_FIELDS + "|" + REGEX_FORWARD + ")";
    private static final String REGEX_ANNOTATION =
            "((" + REGEX_FIELD_OR_FORWARD + ";)*(" + REGEX_FIELD_OR_FORWARD + ");?)";

    private static final Pattern PATTERN_WILDCARD = Pattern.compile(REGEX_WILDCARD);
    private static final Pattern PATTERN_FORWARD = Pattern.compile(REGEX_FORWARD);
    private static final Pattern PATTERN_ANNOTATION = Pattern.compile(REGEX_ANNOTATION);
    private static final Pattern PATTERN_LIST = Pattern.compile(REGEX_LIST);
    private static final Pattern PATTERN_FIELD = Pattern.compile(REGEX_NESTED_FIELDS);

    public static SingleInputSemanticProperties createProjectionPropertiesSingle(
            int[] fields, CompositeType<?> inType) {

        Character.isJavaIdentifierStart(1);

        SingleInputSemanticProperties ssp = new SingleInputSemanticProperties();

        int[] sourceOffsets = new int[inType.getArity()];
        sourceOffsets[0] = 0;
        for (int i = 1; i < inType.getArity(); i++) {
            sourceOffsets[i] = inType.getTypeAt(i - 1).getTotalFields() + sourceOffsets[i - 1];
        }

        int targetOffset = 0;
        for (int i = 0; i < fields.length; i++) {
            int sourceOffset = sourceOffsets[fields[i]];
            int numFieldsToCopy = inType.getTypeAt(fields[i]).getTotalFields();

            for (int j = 0; j < numFieldsToCopy; j++) {
                ssp.addForwardedField(sourceOffset + j, targetOffset + j);
            }
            targetOffset += numFieldsToCopy;
        }

        return ssp;
    }

    public static DualInputSemanticProperties createProjectionPropertiesDual(
            int[] fields,
            boolean[] isFromFirst,
            TypeInformation<?> inType1,
            TypeInformation<?> inType2) {
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();

        int[] sourceOffsets1;
        if (inType1 instanceof TupleTypeInfo<?>) {
            sourceOffsets1 = new int[inType1.getArity()];
            sourceOffsets1[0] = 0;
            for (int i = 1; i < inType1.getArity(); i++) {
                sourceOffsets1[i] =
                        ((TupleTypeInfo<?>) inType1).getTypeAt(i - 1).getTotalFields()
                                + sourceOffsets1[i - 1];
            }
        } else {
            sourceOffsets1 = new int[] {0};
        }

        int[] sourceOffsets2;
        if (inType2 instanceof TupleTypeInfo<?>) {
            sourceOffsets2 = new int[inType2.getArity()];
            sourceOffsets2[0] = 0;
            for (int i = 1; i < inType2.getArity(); i++) {
                sourceOffsets2[i] =
                        ((TupleTypeInfo<?>) inType2).getTypeAt(i - 1).getTotalFields()
                                + sourceOffsets2[i - 1];
            }
        } else {
            sourceOffsets2 = new int[] {0};
        }

        int targetOffset = 0;
        for (int i = 0; i < fields.length; i++) {
            int sourceOffset;
            int numFieldsToCopy;
            int input;
            if (isFromFirst[i]) {
                input = 0;
                if (fields[i] == -1) {
                    sourceOffset = 0;
                    numFieldsToCopy = inType1.getTotalFields();
                } else {
                    sourceOffset = sourceOffsets1[fields[i]];
                    numFieldsToCopy =
                            ((TupleTypeInfo<?>) inType1).getTypeAt(fields[i]).getTotalFields();
                }
            } else {
                input = 1;
                if (fields[i] == -1) {
                    sourceOffset = 0;
                    numFieldsToCopy = inType2.getTotalFields();
                } else {
                    sourceOffset = sourceOffsets2[fields[i]];
                    numFieldsToCopy =
                            ((TupleTypeInfo<?>) inType2).getTypeAt(fields[i]).getTotalFields();
                }
            }

            for (int j = 0; j < numFieldsToCopy; j++) {
                dsp.addForwardedField(input, sourceOffset + j, targetOffset + j);
            }
            targetOffset += numFieldsToCopy;
        }

        return dsp;
    }

    /**
     * Creates SemanticProperties by adding an offset to each input field index of the given
     * SemanticProperties.
     *
     * @param props The SemanticProperties to which the offset is added.
     * @param numInputFields The original number of fields of the input.
     * @param offset The offset that is added to each input field index.
     * @return New SemanticProperties with added offset.
     */
    public static SingleInputSemanticProperties addSourceFieldOffset(
            SingleInputSemanticProperties props, int numInputFields, int offset) {

        SingleInputSemanticProperties offsetProps = new SingleInputSemanticProperties();
        if (props.getReadFields(0) != null) {
            FieldSet offsetReadFields = new FieldSet();
            for (int r : props.getReadFields(0)) {
                offsetReadFields = offsetReadFields.addField(r + offset);
            }
            offsetProps.addReadFields(offsetReadFields);
        }
        for (int s = 0; s < numInputFields; s++) {
            FieldSet targetFields = props.getForwardingTargetFields(0, s);
            for (int t : targetFields) {
                offsetProps.addForwardedField(s + offset, t);
            }
        }
        return offsetProps;
    }

    /**
     * Creates SemanticProperties by adding offsets to each input field index of the given
     * SemanticProperties.
     *
     * @param props The SemanticProperties to which the offset is added.
     * @param numInputFields1 The original number of fields of the first input.
     * @param numInputFields2 The original number of fields of the second input.
     * @param offset1 The offset that is added to each input field index of the first input.
     * @param offset2 The offset that is added to each input field index of the second input.
     * @return New SemanticProperties with added offsets.
     */
    public static DualInputSemanticProperties addSourceFieldOffsets(
            DualInputSemanticProperties props,
            int numInputFields1,
            int numInputFields2,
            int offset1,
            int offset2) {

        DualInputSemanticProperties offsetProps = new DualInputSemanticProperties();

        // add offset to read fields on first input
        if (props.getReadFields(0) != null) {
            FieldSet offsetReadFields = new FieldSet();
            for (int r : props.getReadFields(0)) {
                offsetReadFields = offsetReadFields.addField(r + offset1);
            }
            offsetProps.addReadFields(0, offsetReadFields);
        }
        // add offset to read fields on second input
        if (props.getReadFields(1) != null) {
            FieldSet offsetReadFields = new FieldSet();
            for (int r : props.getReadFields(1)) {
                offsetReadFields = offsetReadFields.addField(r + offset2);
            }
            offsetProps.addReadFields(1, offsetReadFields);
        }

        // add offset to forward fields on first input
        for (int s = 0; s < numInputFields1; s++) {
            FieldSet targetFields = props.getForwardingTargetFields(0, s);
            for (int t : targetFields) {
                offsetProps.addForwardedField(0, s + offset1, t);
            }
        }
        // add offset to forward fields on second input
        for (int s = 0; s < numInputFields2; s++) {
            FieldSet targetFields = props.getForwardingTargetFields(1, s);
            for (int t : targetFields) {
                offsetProps.addForwardedField(1, s + offset2, t);
            }
        }

        return offsetProps;
    }

    public static SingleInputSemanticProperties getSemanticPropsSingle(
            Set<Annotation> set, TypeInformation<?> inType, TypeInformation<?> outType) {
        if (set == null) {
            return null;
        }
        Iterator<Annotation> it = set.iterator();

        String[] forwarded = null;
        String[] nonForwarded = null;
        String[] read = null;

        while (it.hasNext()) {

            Annotation ann = it.next();

            if (ann instanceof ForwardedFields) {
                forwarded = ((ForwardedFields) ann).value();
            } else if (ann instanceof NonForwardedFields) {
                nonForwarded = ((NonForwardedFields) ann).value();
            } else if (ann instanceof ReadFields) {
                read = ((ReadFields) ann).value();
            } else if (ann instanceof ForwardedFieldsFirst
                    || ann instanceof ForwardedFieldsSecond
                    || ann instanceof NonForwardedFieldsFirst
                    || ann instanceof NonForwardedFieldsSecond
                    || ann instanceof ReadFieldsFirst
                    || ann instanceof ReadFieldsSecond) {
                throw new InvalidSemanticAnnotationException(
                        "Annotation " + ann.getClass() + " invalid for single input function.");
            }
        }

        if (forwarded != null || nonForwarded != null || read != null) {
            SingleInputSemanticProperties result = new SingleInputSemanticProperties();
            getSemanticPropsSingleFromString(
                    result, forwarded, nonForwarded, read, inType, outType);
            return result;
        }
        return null;
    }

    public static DualInputSemanticProperties getSemanticPropsDual(
            Set<Annotation> set,
            TypeInformation<?> inType1,
            TypeInformation<?> inType2,
            TypeInformation<?> outType) {
        if (set == null) {
            return null;
        }
        Iterator<Annotation> it = set.iterator();

        String[] forwardedFirst = null;
        String[] forwardedSecond = null;
        String[] nonForwardedFirst = null;
        String[] nonForwardedSecond = null;
        String[] readFirst = null;
        String[] readSecond = null;

        while (it.hasNext()) {
            Annotation ann = it.next();

            if (ann instanceof ForwardedFieldsFirst) {
                forwardedFirst = ((ForwardedFieldsFirst) ann).value();
            } else if (ann instanceof ForwardedFieldsSecond) {
                forwardedSecond = ((ForwardedFieldsSecond) ann).value();
            } else if (ann instanceof NonForwardedFieldsFirst) {
                nonForwardedFirst = ((NonForwardedFieldsFirst) ann).value();
            } else if (ann instanceof NonForwardedFieldsSecond) {
                nonForwardedSecond = ((NonForwardedFieldsSecond) ann).value();
            } else if (ann instanceof ReadFieldsFirst) {
                readFirst = ((ReadFieldsFirst) ann).value();
            } else if (ann instanceof ReadFieldsSecond) {
                readSecond = ((ReadFieldsSecond) ann).value();
            } else if (ann instanceof ForwardedFields
                    || ann instanceof NonForwardedFields
                    || ann instanceof ReadFields) {
                throw new InvalidSemanticAnnotationException(
                        "Annotation " + ann.getClass() + " invalid for dual input function.");
            }
        }

        if (forwardedFirst != null
                || nonForwardedFirst != null
                || readFirst != null
                || forwardedSecond != null
                || nonForwardedSecond != null
                || readSecond != null) {
            DualInputSemanticProperties result = new DualInputSemanticProperties();
            getSemanticPropsDualFromString(
                    result,
                    forwardedFirst,
                    forwardedSecond,
                    nonForwardedFirst,
                    nonForwardedSecond,
                    readFirst,
                    readSecond,
                    inType1,
                    inType2,
                    outType);
            return result;
        }
        return null;
    }

    public static void getSemanticPropsSingleFromString(
            SingleInputSemanticProperties result,
            String[] forwarded,
            String[] nonForwarded,
            String[] readSet,
            TypeInformation<?> inType,
            TypeInformation<?> outType) {
        getSemanticPropsSingleFromString(
                result, forwarded, nonForwarded, readSet, inType, outType, false);
    }

    public static void getSemanticPropsSingleFromString(
            SingleInputSemanticProperties result,
            String[] forwarded,
            String[] nonForwarded,
            String[] readSet,
            TypeInformation<?> inType,
            TypeInformation<?> outType,
            boolean skipIncompatibleTypes) {

        boolean hasForwardedAnnotation = false;
        boolean hasNonForwardedAnnotation = false;
        // check for forwarded annotations
        if (forwarded != null && forwarded.length > 0) {
            hasForwardedAnnotation = true;
        }
        // check non-forwarded annotations
        if (nonForwarded != null && nonForwarded.length > 0) {
            hasNonForwardedAnnotation = true;
        }

        if (hasForwardedAnnotation && hasNonForwardedAnnotation) {
            throw new InvalidSemanticAnnotationException(
                    "Either ForwardedFields OR "
                            + "NonForwardedFields annotation permitted, NOT both.");
        } else if (hasForwardedAnnotation) {
            parseForwardedFields(result, forwarded, inType, outType, 0, skipIncompatibleTypes);
        } else if (hasNonForwardedAnnotation) {
            parseNonForwardedFields(
                    result, nonForwarded, inType, outType, 0, skipIncompatibleTypes);
        }
        parseReadFields(result, readSet, inType, 0);
    }

    public static void getSemanticPropsDualFromString(
            DualInputSemanticProperties result,
            String[] forwardedFirst,
            String[] forwardedSecond,
            String[] nonForwardedFirst,
            String[] nonForwardedSecond,
            String[] readFieldsFirst,
            String[] readFieldsSecond,
            TypeInformation<?> inType1,
            TypeInformation<?> inType2,
            TypeInformation<?> outType) {
        getSemanticPropsDualFromString(
                result,
                forwardedFirst,
                forwardedSecond,
                nonForwardedFirst,
                nonForwardedSecond,
                readFieldsFirst,
                readFieldsSecond,
                inType1,
                inType2,
                outType,
                false);
    }

    public static void getSemanticPropsDualFromString(
            DualInputSemanticProperties result,
            String[] forwardedFirst,
            String[] forwardedSecond,
            String[] nonForwardedFirst,
            String[] nonForwardedSecond,
            String[] readFieldsFirst,
            String[] readFieldsSecond,
            TypeInformation<?> inType1,
            TypeInformation<?> inType2,
            TypeInformation<?> outType,
            boolean skipIncompatibleTypes) {

        boolean hasForwardedFirstAnnotation = false;
        boolean hasForwardedSecondAnnotation = false;
        boolean hasNonForwardedFirstAnnotation = false;
        boolean hasNonForwardedSecondAnnotation = false;
        // check for forwarded annotations
        if (forwardedFirst != null && forwardedFirst.length > 0) {
            hasForwardedFirstAnnotation = true;
        }
        if (forwardedSecond != null && forwardedSecond.length > 0) {
            hasForwardedSecondAnnotation = true;
        }
        // check non-forwarded annotations
        if (nonForwardedFirst != null && nonForwardedFirst.length > 0) {
            hasNonForwardedFirstAnnotation = true;
        }
        if (nonForwardedSecond != null && nonForwardedSecond.length > 0) {
            hasNonForwardedSecondAnnotation = true;
        }

        if (hasForwardedFirstAnnotation && hasNonForwardedFirstAnnotation) {
            throw new InvalidSemanticAnnotationException(
                    "Either ForwardedFieldsFirst OR "
                            + "NonForwardedFieldsFirst annotation permitted, NOT both.");
        }
        if (hasForwardedSecondAnnotation && hasNonForwardedSecondAnnotation) {
            throw new InvalidSemanticAnnotationException(
                    "Either ForwardedFieldsSecond OR "
                            + "NonForwardedFieldsSecond annotation permitted, NOT both.");
        }

        if (hasForwardedFirstAnnotation) {
            parseForwardedFields(
                    result, forwardedFirst, inType1, outType, 0, skipIncompatibleTypes);
        } else if (hasNonForwardedFirstAnnotation) {
            parseNonForwardedFields(
                    result, nonForwardedFirst, inType1, outType, 0, skipIncompatibleTypes);
        }

        if (hasForwardedSecondAnnotation) {
            parseForwardedFields(
                    result, forwardedSecond, inType2, outType, 1, skipIncompatibleTypes);
        } else if (hasNonForwardedSecondAnnotation) {
            parseNonForwardedFields(
                    result, nonForwardedSecond, inType2, outType, 1, skipIncompatibleTypes);
        }

        parseReadFields(result, readFieldsFirst, inType1, 0);
        parseReadFields(result, readFieldsSecond, inType2, 1);
    }

    private static void parseForwardedFields(
            SemanticProperties sp,
            String[] forwardedStr,
            TypeInformation<?> inType,
            TypeInformation<?> outType,
            int input,
            boolean skipIncompatibleTypes) {

        if (forwardedStr == null) {
            return;
        }

        for (String s : forwardedStr) {
            if (s == null) {
                continue;
            }

            // remove white characters
            s = s.replaceAll("\\s", "");

            Matcher wcMatcher = PATTERN_WILDCARD.matcher(s);
            // simple wildcard
            if (wcMatcher.matches()) {

                if (!inType.equals(outType)) {
                    if (skipIncompatibleTypes) {
                        continue;
                    } else {
                        throw new InvalidSemanticAnnotationException(
                                "Forwarded field annotation \""
                                        + s
                                        + "\" with wildcard only allowed for identical input and output types.");
                    }
                }

                for (int i = 0; i < inType.getTotalFields(); i++) {
                    if (sp instanceof SingleInputSemanticProperties) {
                        ((SingleInputSemanticProperties) sp).addForwardedField(i, i);
                    } else if (sp instanceof DualInputSemanticProperties) {
                        ((DualInputSemanticProperties) sp).addForwardedField(input, i, i);
                    }
                }
                return;
            }

            // check format of annotation string
            Matcher matcher = PATTERN_ANNOTATION.matcher(s);
            if (!matcher.matches()) {
                throw new InvalidSemanticAnnotationException(
                        "Invalid format of forwarded field annotation \"" + s + "\".");
            }

            // add forward annotations "->"
            Matcher forwardMatcher = PATTERN_FORWARD.matcher(s);
            while (forwardMatcher.find()) {
                String sourceStr = forwardMatcher.group(2);
                String targetStr = forwardMatcher.group(6);

                try {
                    // check type compatibility
                    if (!areFieldsCompatible(
                            sourceStr, inType, targetStr, outType, !skipIncompatibleTypes)) {
                        if (skipIncompatibleTypes) {
                            continue;
                        } else {
                            throw new InvalidSemanticAnnotationException(
                                    "Referenced fields of forwarded field annotation \""
                                            + s
                                            + "\" do not match.");
                        }
                    }
                    List<FlatFieldDescriptor> inFFDs = getFlatFields(sourceStr, inType);
                    List<FlatFieldDescriptor> outFFDs = getFlatFields(targetStr, outType);
                    if (sp instanceof SingleInputSemanticProperties) {
                        for (int i = 0; i < inFFDs.size(); i++) {
                            int sourceField = inFFDs.get(i).getPosition();
                            int targetField = outFFDs.get(i).getPosition();
                            ((SingleInputSemanticProperties) sp)
                                    .addForwardedField(sourceField, targetField);
                        }
                    } else if (sp instanceof DualInputSemanticProperties) {
                        for (int i = 0; i < inFFDs.size(); i++) {
                            int sourceField = inFFDs.get(i).getPosition();
                            int targetField = outFFDs.get(i).getPosition();
                            ((DualInputSemanticProperties) sp)
                                    .addForwardedField(input, sourceField, targetField);
                        }
                    }
                } catch (InvalidFieldReferenceException ifre) {
                    throw new InvalidSemanticAnnotationException(
                            "Invalid field reference in forwarded field annotation \""
                                    + sourceStr
                                    + "->"
                                    + targetStr
                                    + "\".",
                            ifre);
                } catch (InvalidSemanticAnnotationException isae) {
                    throw new InvalidSemanticAnnotationException(
                            "Forwarded field annotation \""
                                    + sourceStr
                                    + "->"
                                    + targetStr
                                    + "\" could not be added.",
                            isae);
                }
            }
            // remove forward annotations
            s = forwardMatcher.replaceAll("");

            // add forwarded annotations
            Matcher listMatcher = PATTERN_LIST.matcher(s);
            while (listMatcher.find()) {
                String list = listMatcher.group();
                Matcher fieldMatcher = PATTERN_FIELD.matcher(list);

                // for each nested field
                while (fieldMatcher.find()) {
                    String fieldStr = fieldMatcher.group();
                    try {
                        // check if field is compatible in input and output type
                        if (!areFieldsCompatible(
                                fieldStr, inType, fieldStr, outType, !skipIncompatibleTypes)) {
                            if (skipIncompatibleTypes) {
                                continue;
                            } else {
                                throw new InvalidSemanticAnnotationException(
                                        "Referenced fields of forwarded field annotation \""
                                                + s
                                                + "\" do not match.");
                            }
                        }
                        // add flat field positions
                        List<FlatFieldDescriptor> inFFDs = getFlatFields(fieldStr, inType);
                        List<FlatFieldDescriptor> outFFDs = getFlatFields(fieldStr, outType);
                        for (int i = 0; i < inFFDs.size(); i++) {
                            int sourcePos = inFFDs.get(i).getPosition();
                            int targetPos = outFFDs.get(i).getPosition();
                            if (sp instanceof SingleInputSemanticProperties) {
                                ((SingleInputSemanticProperties) sp)
                                        .addForwardedField(sourcePos, targetPos);
                            } else if (sp instanceof DualInputSemanticProperties) {
                                ((DualInputSemanticProperties) sp)
                                        .addForwardedField(input, sourcePos, targetPos);
                            }
                        }
                    } catch (InvalidFieldReferenceException ifre) {
                        throw new InvalidSemanticAnnotationException(
                                "Invalid field reference in forwarded field annotation \""
                                        + fieldStr
                                        + "\".",
                                ifre);
                    } catch (InvalidSemanticAnnotationException isae) {
                        throw new InvalidSemanticAnnotationException(
                                "Forwarded field annotation \""
                                        + fieldStr
                                        + "\" could not be added.",
                                isae);
                    }
                }
            }
        }
    }

    private static void parseNonForwardedFields(
            SemanticProperties sp,
            String[] nonForwardedStr,
            TypeInformation<?> inType,
            TypeInformation<?> outType,
            int input,
            boolean skipIncompatibleTypes) {

        if (nonForwardedStr == null) {
            return;
        }

        FieldSet excludedFields = new FieldSet();
        for (String s : nonForwardedStr) {

            // remove white characters
            s = s.replaceAll("\\s", "");

            if (s.equals("")) {
                continue;
            }

            if (!inType.equals(outType)) {
                if (skipIncompatibleTypes) {
                    continue;
                } else {
                    throw new InvalidSemanticAnnotationException(
                            "Non-forwarded fields annotation only allowed for identical input and output types.");
                }
            }

            Matcher matcher = PATTERN_LIST.matcher(s);
            if (!matcher.matches()) {
                throw new InvalidSemanticAnnotationException(
                        "Invalid format of non-forwarded fields annotation \"" + s + "\".");
            }

            // process individual fields
            matcher = PATTERN_FIELD.matcher(s);
            while (matcher.find()) {
                String fieldStr = matcher.group();

                try {
                    // get and add all flat field positions
                    List<FlatFieldDescriptor> inFFDs = getFlatFields(fieldStr, inType);
                    for (FlatFieldDescriptor ffd : inFFDs) {
                        excludedFields = excludedFields.addField(ffd.getPosition());
                    }
                } catch (InvalidFieldReferenceException ifre) {
                    throw new InvalidSemanticAnnotationException(
                            "Invalid field reference in non-forwarded fields annotation \""
                                    + fieldStr
                                    + "\".",
                            ifre);
                }
            }
        }

        for (int i = 0; i < inType.getTotalFields(); i++) {
            if (!excludedFields.contains(i)) {
                if (sp instanceof SingleInputSemanticProperties) {
                    ((SingleInputSemanticProperties) sp).addForwardedField(i, i);
                } else if (sp instanceof DualInputSemanticProperties) {
                    ((DualInputSemanticProperties) sp).addForwardedField(input, i, i);
                }
            }
        }
    }

    private static void parseReadFields(
            SemanticProperties sp,
            String[] readFieldStrings,
            TypeInformation<?> inType,
            int input) {

        if (readFieldStrings == null) {
            return;
        }

        for (String s : readFieldStrings) {

            FieldSet readFields = new FieldSet();

            // remove white characters
            s = s.replaceAll("\\s", "");

            Matcher wcMatcher = PATTERN_WILDCARD.matcher(s);
            // simple wildcard
            if (wcMatcher.matches()) {
                // add all fields
                for (int i = 0; i < inType.getTotalFields(); i++) {
                    readFields = readFields.addField(i);
                }
            } else {
                // process field list
                Matcher matcher = PATTERN_LIST.matcher(s);

                if (!matcher.matches()) {
                    throw new InvalidSemanticAnnotationException(
                            "Invalid format of read field annotation \"" + s + "\".");
                }

                // process field
                matcher = PATTERN_FIELD.matcher(s);
                while (matcher.find()) {
                    String fieldStr = matcher.group();
                    try {
                        List<FlatFieldDescriptor> ffds = getFlatFields(fieldStr, inType);
                        // get and add flat field positions
                        for (FlatFieldDescriptor ffd : ffds) {
                            readFields = readFields.addField(ffd.getPosition());
                        }
                    } catch (InvalidFieldReferenceException ifre) {
                        throw new InvalidSemanticAnnotationException(
                                "Invalid field reference in read field annotation \""
                                        + fieldStr
                                        + "\".",
                                ifre);
                    }
                }
            }

            if (sp instanceof SingleInputSemanticProperties) {
                ((SingleInputSemanticProperties) sp).addReadFields(readFields);
            } else if (sp instanceof DualInputSemanticProperties) {
                ((DualInputSemanticProperties) sp).addReadFields(input, readFields);
            }
        }
    }

    ////////////////////// UTIL METHODS ///////////////////////////////

    private static boolean areFieldsCompatible(
            String sourceField,
            TypeInformation<?> inType,
            String targetField,
            TypeInformation<?> outType,
            boolean throwException) {

        try {
            // get source type information
            TypeInformation<?> sourceType = getExpressionTypeInformation(sourceField, inType);
            // get target type information
            TypeInformation<?> targetType = getExpressionTypeInformation(targetField, outType);
            return sourceType.equals(targetType);
        } catch (InvalidFieldReferenceException e) {
            if (throwException) {
                throw e;
            } else {
                return false;
            }
        }
    }

    private static TypeInformation<?> getExpressionTypeInformation(
            String fieldStr, TypeInformation<?> typeInfo) {
        Matcher wildcardMatcher = PATTERN_WILDCARD.matcher(fieldStr);
        if (wildcardMatcher.matches()) {
            return typeInfo;
        } else {
            Matcher expMatcher = PATTERN_FIELD.matcher(fieldStr);
            if (!expMatcher.matches()) {
                throw new InvalidFieldReferenceException(
                        "Invalid field expression \"" + fieldStr + "\".");
            }
            if (typeInfo instanceof CompositeType<?>) {
                return ((CompositeType<?>) typeInfo).getTypeAt(expMatcher.group(1));
            } else {
                throw new InvalidFieldReferenceException(
                        "Nested field expression \""
                                + fieldStr
                                + "\" not possible on atomic type ("
                                + typeInfo
                                + ").");
            }
        }
    }

    private static List<FlatFieldDescriptor> getFlatFields(
            String fieldStr, TypeInformation<?> typeInfo) {
        if (typeInfo instanceof CompositeType<?>) {
            return ((CompositeType<?>) typeInfo).getFlatFields(fieldStr);
        } else {
            Matcher wildcardMatcher = PATTERN_WILDCARD.matcher(fieldStr);
            if (wildcardMatcher.matches()) {
                return Collections.singletonList(new FlatFieldDescriptor(0, typeInfo));
            } else {
                throw new InvalidFieldReferenceException(
                        "Nested field expression \""
                                + fieldStr
                                + "\" not possible on atomic type ("
                                + typeInfo
                                + ").");
            }
        }
    }

    /** Private constructor to prevent instantiation. */
    private SemanticPropUtil() {
        throw new RuntimeException();
    }
}
