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

package org.apache.flink.formats.protobuf.util;

import org.apache.flink.annotation.VisibleForTesting;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protobuf field conflict resolver for Java code generation.
 *
 * <p>Implements protoc 4.x field conflict semantics: we derive all potential field accessor names
 * (base, enum numeric, repeated list/count) and suffix every field base in a conflict group with
 * its field number when any clash is detected. This covers field conflicts between:
 *
 * <ul>
 *   <li>Any *_value fields vs enum numeric value accessors
 *   <li>Any *_list / *_count fields vs repeated field list/count accessors
 * </ul>
 *
 * <p>We explicitly mirror only the supported conflict resolution scenarios implemented by the
 * Protobuf Java compiler to maintain 1:1 compatibility. Quoting the official implementation
 * (https://github.com/protocolbuffers/protobuf/blob/v32.1/src/google/protobuf/compiler/java/context.cc#L93-L97):
 * "Well, there are obviously many more conflicting cases, but it probably doesn't worth the effort
 * to exhaust all of them because they rarely happen and as we are continuing adding new
 * methods/changing existing methods the number of different conflicting cases will keep growing. We
 * can just add more cases here when they are found in the real world."
 *
 * <p>Note: Only a single cache keyed by {@link Descriptors.Descriptor} is maintained. Public
 * helpers expose resolved getter variants, repeated count, enum numeric accessors, builder setter
 * segment derivation, and presence/non-default checks used by Flink's dynamic codegen.
 */
public class PbFieldConflictResolver {

    // Accessor mapping implementation
    public static final class AccessorNames {
        public final String getter; // getBase or getBaseN
        public final String enumNumericGetter; // getBaseValue or getBaseNValue
        public final String repeatedListGetter; // getBaseList or getBaseNList
        public final String repeatedCountGetter; // getBaseCount or getBaseNCount

        AccessorNames(String g, String e, String rl, String rc) {
            this.getter = g;
            this.enumNumericGetter = e;
            this.repeatedListGetter = rl;
            this.repeatedCountGetter = rc;
        }
    }

    public static final class AccessorMappings {
        final Map<Integer, AccessorNames> map;

        AccessorMappings(Map<Integer, AccessorNames> map) {
            this.map = map;
        }

        public AccessorNames get(FieldDescriptor fd) {
            return map.get(fd.getNumber());
        }
    }

    private static final ConcurrentHashMap<Descriptors.Descriptor, AccessorMappings> CACHE =
            new ConcurrentHashMap<>();

    public static AccessorMappings getAccessorMappings(Descriptors.Descriptor desc) {
        return CACHE.computeIfAbsent(desc, PbFieldConflictResolver::buildMappings);
    }

    private static AccessorMappings buildMappings(Descriptors.Descriptor desc) {
        // Phase 1: collect potential (unsuffixed) accessor method names per field.
        Map<Integer, String> baseByFieldNumber = new HashMap<>(desc.getFields().size());
        Map<String, List<FieldDescriptor>> potentialAccessorToFields = new HashMap<>();
        for (FieldDescriptor fd : desc.getFields()) {
            String base = PbFormatUtils.getStrongCamelCaseJsonName(fd.getJsonName());
            int fieldNumber = fd.getNumber();
            baseByFieldNumber.put(fieldNumber, base);
            potentialAccessorToFields.computeIfAbsent("get" + base, k -> new ArrayList<>()).add(fd);
            if (fd.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                potentialAccessorToFields
                        .computeIfAbsent("get" + base + "Value", k -> new ArrayList<>())
                        .add(fd);
            }
            if (fd.isRepeated() && !fd.isMapField()) {
                potentialAccessorToFields
                        .computeIfAbsent("get" + base + "List", k -> new ArrayList<>())
                        .add(fd);
                potentialAccessorToFields
                        .computeIfAbsent("get" + base + "Count", k -> new ArrayList<>())
                        .add(fd);
            }
        }

        // Phase 2: detect field conflicts by shared accessor names.
        Set<String> conflictingBases = new HashSet<>();
        for (Map.Entry<String, List<FieldDescriptor>> e : potentialAccessorToFields.entrySet()) {
            if (e.getValue().size() > 1) {
                for (FieldDescriptor fd : e.getValue()) {
                    conflictingBases.add(
                            PbFormatUtils.getStrongCamelCaseJsonName(fd.getJsonName()));
                }
            }
        }

        // Phase 3: build final mappings with suffixes applied to conflicting bases.
        Map<Integer, AccessorNames> mappings = new HashMap<>();
        for (FieldDescriptor fd : desc.getFields()) {
            int fieldNumber = fd.getNumber();
            String base = baseByFieldNumber.get(fieldNumber);
            boolean conflict = conflictingBases.contains(base);
            String effectiveBase = conflict ? base + fieldNumber : base;
            String getter = "get" + effectiveBase;
            String enumNumeric = null;
            String listGetter = null;
            String countGetter = null;
            if (fd.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                enumNumeric = "get" + effectiveBase + "Value";
            }
            if (fd.isRepeated() && !fd.isMapField()) {
                listGetter = "get" + effectiveBase + "List";
                countGetter = "get" + effectiveBase + "Count";
            }
            mappings.put(
                    fieldNumber, new AccessorNames(getter, enumNumeric, listGetter, countGetter));
        }
        return new AccessorMappings(mappings);
    }

    /**
     * Resolves the appropriate getter name for the field.
     *
     * @param fd field descriptor
     * @param isList whether caller wants the repeated list accessor for a repeated field
     * @param m precomputed accessor mappings
     * @return method name like getField(), getFieldN(), getFieldNList(), getFieldMap()
     */
    public static String resolveGetter(FieldDescriptor fd, boolean isList, AccessorMappings m) {
        AccessorNames names = m.get(fd);
        if (fd.isMapField()) {
            String camel = PbFormatUtils.getStrongCamelCaseJsonName(fd.getJsonName());
            return "get" + camel + "Map"; // map accessor naming
        }
        if (isList && names.repeatedListGetter != null) {
            return names.repeatedListGetter;
        }
        if (fd.getJavaType() == FieldDescriptor.JavaType.ENUM) {
            return names.getter; // caller decides if numeric needed via resolveEnumNumeric
        }
        return names.getter;
    }

    /**
     * Returns the enum numeric accessor for an enum field, or null for non-enum fields. Returns
     * get{FieldName}Value (no conflict) or get{FieldName}{FieldNumber}Value (conflict resolution
     * applied).
     */
    public static String resolveEnumNumeric(FieldDescriptor fd, AccessorMappings m) {
        AccessorNames names = m.get(fd);
        return names.enumNumericGetter;
    }

    /**
     * Returns the repeated count accessor for a repeated non-map field, or null otherwise. Returns
     * get{FieldName}Count (no conflict) or get{FieldName}{FieldNumber}Count (conflict resolution
     * applied).
     */
    public static String resolveRepeatedCount(FieldDescriptor fd, AccessorMappings m) {
        AccessorNames names = m.get(fd);
        return names.repeatedCountGetter;
    }

    /**
     * Derives the builder setter segment (without set/addAll prefix) from the resolved getter.
     * Example: getTags4List() -> Tags4List, getMode13() -> Mode13.
     */
    public static String resolveSetterSegment(
            FieldDescriptor fd, String fieldName, AccessorMappings mappings) {
        AccessorNames names = mappings.get(fd);
        String getter = names.getter;
        if (getter.startsWith("get")) {
            return getter.substring(3);
        }
        return PbFormatUtils.getStrongCamelCaseJsonName(fieldName);
    }

    /**
     * Generates a boolean Java expression that determines whether the field is present/non-default.
     * For repeated/map fields we check count>0; for proto3 scalars without presence we compare
     * against default value; for enums we check numeric value non-zero.
     *
     * @param message variable name of the message instance
     * @param fd field descriptor
     * @param strongCamelFieldName camel-cased JSON name (used for hasX / fallback count)
     * @param isListOrMap whether caller treats field as collection
     * @param mappings accessor mappings
     * @return Java expression string evaluating to a boolean
     */
    public static String generatePresenceCheck(
            String message,
            FieldDescriptor fd,
            String strongCamelFieldName,
            boolean isListOrMap,
            AccessorMappings mappings) {

        if (isListOrMap) {
            return generateCountCheck(message, fd, strongCamelFieldName, mappings);
        }

        if (fd.hasPresence()) {
            return generateHasCheck(message, fd, strongCamelFieldName, mappings);
        }

        // For fields without explicit presence, check for non-default values
        return generateNonDefaultCheck(message, fd, strongCamelFieldName, mappings);
    }

    private static String generateCountCheck(
            String message,
            FieldDescriptor fd,
            String strongCamelFieldName,
            AccessorMappings mappings) {
        AccessorNames names = mappings.get(fd);
        String countGetter =
                names.repeatedCountGetter != null
                        ? names.repeatedCountGetter
                        : ("get" + strongCamelFieldName + "Count");
        return message + "." + countGetter + "() > 0";
    }

    private static String generateHasCheck(
            String message,
            FieldDescriptor fd,
            String strongCamelFieldName,
            AccessorMappings mappings) {
        // Presence remains based on base camel name; simplified resolver always transforms all
        // accessors within a field conflict group so hasX remains valid (protoc behavior). We do
        // not attempt to suffix presence checks here.
        return message + ".has" + strongCamelFieldName + "()";
    }

    private static String generateNonDefaultCheck(
            String message,
            FieldDescriptor fd,
            String strongCamelFieldName,
            AccessorMappings mappings) {
        AccessorNames names = mappings.get(fd);
        String getterCall = message + "." + names.getter + "()";

        switch (fd.getJavaType()) {
            case STRING:
                return getterCall + ".length() > 0";
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return getterCall + " != 0";
            case BOOLEAN:
                return getterCall;
            case BYTE_STRING:
                return getterCall + ".size() > 0";
            case ENUM:
                if (fd.getType() == FieldDescriptor.Type.ENUM && !fd.hasPresence()) {
                    return getterCall + ".getNumber() != 0";
                }
                return getterCall + " != 0";
            default:
                return getterCall + " != null";
        }
    }

    /** Clears internal descriptor->accessor mapping cache (testing / lifecycle). */
    @VisibleForTesting
    public static void clearCache() {
        CACHE.clear();
    }
}
