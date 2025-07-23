/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.util;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.runtime.ConsList;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.LibraryOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Flink modifications
 *
 * <p>Lines 127 ~ 145 to mitigate the impact of CALCITE-6024.
 */
public abstract class ReflectiveSqlOperatorTable extends SqlOperatorTables.IndexedSqlOperatorTable
        implements SqlOperatorTable {
    public static final String IS_NAME = "INFORMATION_SCHEMA";

    // ~ Instance fields --------------------------------------------------------

    // ~ Constructors -----------------------------------------------------------

    protected ReflectiveSqlOperatorTable() {
        // Initialize using an empty list of operators. After construction is
        // complete we will call init() and set the true operator list.
        super(ImmutableList.of());
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * Performs post-constructor initialization of an operator table. It can't be part of the
     * constructor, because the subclass constructor needs to complete first.
     */
    public final SqlOperatorTable init() {
        // Use reflection to register the expressions stored in public fields.
        final List<SqlOperator> list = new ArrayList<>();
        for (Field field : getClass().getFields()) {
            try {
                final Object o = field.get(this);
                if (o instanceof SqlOperator) {
                    // Fields do not need the LibraryOperator tag, but if they have it,
                    // we index them only if they contain STANDARD library.
                    LibraryOperator libraryOperator = field.getAnnotation(LibraryOperator.class);
                    if (libraryOperator != null) {
                        if (Arrays.stream(libraryOperator.libraries())
                                .noneMatch(library -> library == SqlLibrary.STANDARD)) {
                            continue;
                        }
                    }

                    list.add((SqlOperator) o);
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e));
            }
        }
        setOperators(buildIndex(list));
        return this;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            @Nullable SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        // NOTE jvs 3-Mar-2005:  ignore category until someone cares

        String simpleName;
        if (opName.names.size() > 1) {
            if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {
                // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
                simpleName = Util.last(opName.names);
            } else {
                return;
            }
        } else {
            simpleName = opName.getSimple();
        }

        lookUpOperators(
                simpleName,
                nameMatcher.isCaseSensitive(),
                op -> {
                    if (op.getSyntax() == syntax) {
                        operatorList.add(op);
                    } else if (syntax == SqlSyntax.FUNCTION && op instanceof SqlFunction) {
                        // this special case is needed for operators like CAST,
                        // which are treated as functions but have special syntax
                        operatorList.add(op);
                    }
                });

        // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
        // Shouldn't it be covered by search above?
        /*
        // BEGIN FLINK MODIFICATION
        switch (syntax) {
            case BINARY:
            case PREFIX:
            case POSTFIX:
                lookUpOperators(
                        simpleName,
                        nameMatcher.isCaseSensitive(),
                        extra -> {
                            // REVIEW: should only search operators added during this method?
                            if (extra != null && !operatorList.contains(extra)) {
                                operatorList.add(extra);
                            }
                        });
                break;
            default:
                break;
        }
        // BEGIN FLINK MODIFICATION
         */
    }

    /**
     * Registers a function or operator in the table.
     *
     * @deprecated This table is designed to be initialized from the fields of a class, and adding
     *     operators is not efficient
     */
    @Deprecated
    public void register(SqlOperator op) {
        // Rebuild the immutable collections with their current contents plus one.
        setOperators(buildIndex(ConsList.of(op, getOperatorList())));
    }
}
