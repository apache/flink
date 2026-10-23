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

package org.apache.flink.table.runtime.operators.join.stream.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link JoinInputSideSpec} is ap specification which describes input side information of a
 * Join.
 */
public class JoinInputSideSpec implements Serializable {
    private static final long serialVersionUID = 3178408082297179959L;

    private final boolean inputSideHasUniqueKey;
    private final boolean joinKeyContainsUniqueKey;
    private final boolean uniqueKeyContainsJoinKey;
    @Nullable private final InternalTypeInfo<RowData> uniqueKeyType;
    @Nullable private final KeySelector<RowData, RowData> uniqueKeySelector;

    private JoinInputSideSpec(
            boolean joinKeyContainsUniqueKey,
            boolean uniqueKeyContainsJoinKey,
            @Nullable InternalTypeInfo<RowData> uniqueKeyType,
            @Nullable KeySelector<RowData, RowData> uniqueKeySelector) {
        this.inputSideHasUniqueKey = uniqueKeyType != null && uniqueKeySelector != null;
        this.joinKeyContainsUniqueKey = joinKeyContainsUniqueKey;
        this.uniqueKeyContainsJoinKey = uniqueKeyContainsJoinKey;
        this.uniqueKeyType = uniqueKeyType;
        this.uniqueKeySelector = uniqueKeySelector;
    }

    /** Returns true if the input has unique key, otherwise false. */
    public boolean hasUniqueKey() {
        return inputSideHasUniqueKey;
    }

    /** Returns true if the join key contains the unique key of the input. */
    public boolean joinKeyContainsUniqueKey() {
        return joinKeyContainsUniqueKey;
    }

    /**
     * Returns true if the unique key of the input is known to contain the join key, so that all
     * versions of a record have the same join key.
     */
    public boolean uniqueKeyContainsJoinKey() {
        return uniqueKeyContainsJoinKey;
    }

    /**
     * Returns the {@link TypeInformation} of the unique key. Returns null if the input hasn't
     * unique key.
     */
    @Nullable
    public InternalTypeInfo<RowData> getUniqueKeyType() {
        return uniqueKeyType;
    }

    /**
     * Returns the {@link KeySelector} to extract unique key from the input row. Returns null if the
     * input hasn't unique key.
     */
    @Nullable
    public KeySelector<RowData, RowData> getUniqueKeySelector() {
        return uniqueKeySelector;
    }

    /**
     * Creates a {@link JoinInputSideSpec} that the input has an unique key.
     *
     * @param uniqueKeyType type information of the unique key
     * @param uniqueKeySelector key selector to extract unique key from the input row
     */
    public static JoinInputSideSpec withUniqueKey(
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector) {
        return withUniqueKey(uniqueKeyType, uniqueKeySelector, false);
    }

    /**
     * Creates a {@link JoinInputSideSpec} that the input has an unique key.
     *
     * @param uniqueKeyType type information of the unique key
     * @param uniqueKeySelector key selector to extract unique key from the input row
     * @param uniqueKeyContainsJoinKey whether the unique key contains the join key
     */
    public static JoinInputSideSpec withUniqueKey(
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector,
            boolean uniqueKeyContainsJoinKey) {
        checkNotNull(uniqueKeyType);
        checkNotNull(uniqueKeySelector);
        return new JoinInputSideSpec(
                false, uniqueKeyContainsJoinKey, uniqueKeyType, uniqueKeySelector);
    }

    /**
     * Creates a {@link JoinInputSideSpec} that input has an unique key and the unique key is
     * contained by the join key.
     *
     * @param uniqueKeyType type information of the unique key
     * @param uniqueKeySelector key selector to extract unique key from the input row
     */
    public static JoinInputSideSpec withUniqueKeyContainedByJoinKey(
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector) {
        return withUniqueKeyContainedByJoinKey(uniqueKeyType, uniqueKeySelector, false);
    }

    /**
     * Creates a {@link JoinInputSideSpec} that input has an unique key and the unique key is
     * contained by the join key.
     *
     * @param uniqueKeyType type information of the unique key
     * @param uniqueKeySelector key selector to extract unique key from the input row
     * @param uniqueKeyContainsJoinKey whether the unique key also contains the join key
     */
    public static JoinInputSideSpec withUniqueKeyContainedByJoinKey(
            InternalTypeInfo<RowData> uniqueKeyType,
            KeySelector<RowData, RowData> uniqueKeySelector,
            boolean uniqueKeyContainsJoinKey) {
        checkNotNull(uniqueKeyType);
        checkNotNull(uniqueKeySelector);
        return new JoinInputSideSpec(
                true, uniqueKeyContainsJoinKey, uniqueKeyType, uniqueKeySelector);
    }

    /** Creates a {@link JoinInputSideSpec} that input hasn't any unique keys. */
    public static JoinInputSideSpec withoutUniqueKey() {
        return new JoinInputSideSpec(false, false, null, null);
    }

    @Override
    public String toString() {
        if (inputSideHasUniqueKey) {
            if (joinKeyContainsUniqueKey) {
                return "JoinKeyContainsUniqueKey";
            } else {
                return "HasUniqueKey";
            }
        } else {
            return "NoUniqueKey";
        }
    }
}
