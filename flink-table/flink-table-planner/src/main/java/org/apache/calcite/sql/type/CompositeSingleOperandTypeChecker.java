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
package org.apache.calcite.sql.type;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Default implementation of {@link org.apache.calcite.sql.type.CompositeOperandTypeChecker}, the
 * class was copied over because of current Calcite issue CALCITE-5380.
 *
 * <p>Lines 73 ~ 79, 101 ~ 107
 */
public class CompositeSingleOperandTypeChecker extends CompositeOperandTypeChecker
        implements SqlSingleOperandTypeChecker {

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a CompositeSingleOperandTypeChecker. Outside this package, use {@link
     * SqlSingleOperandTypeChecker#and(SqlSingleOperandTypeChecker)}, {@link OperandTypes#and},
     * {@link OperandTypes#or} and similar.
     */
    CompositeSingleOperandTypeChecker(
            CompositeOperandTypeChecker.Composition composition,
            ImmutableList<? extends SqlSingleOperandTypeChecker> allowedRules,
            @Nullable String allowedSignatures) {
        super(composition, allowedRules, allowedSignatures, null, null);
    }

    // ~ Methods ----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    public ImmutableList<? extends SqlSingleOperandTypeChecker> getRules() {
        return (ImmutableList<? extends SqlSingleOperandTypeChecker>) allowedRules;
    }

    @Override
    public boolean checkSingleOperandType(
            SqlCallBinding callBinding, SqlNode node, int iFormalOperand, boolean throwOnFailure) {
        assert allowedRules.size() >= 1;

        final ImmutableList<? extends SqlSingleOperandTypeChecker> rules = getRules();
        if (composition == Composition.SEQUENCE) {
            return rules.get(iFormalOperand)
                    .checkSingleOperandType(callBinding, node, 0, throwOnFailure);
        }

        int typeErrorCount = 0;

        boolean throwOnAndFailure = (composition == Composition.AND) && throwOnFailure;

        for (SqlSingleOperandTypeChecker rule : rules) {
            if (!rule.checkSingleOperandType(
                    // FLINK MODIFICATION BEGIN
                    callBinding,
                    node,
                    rule.getClass() == FamilyOperandTypeChecker.class ? 0 : iFormalOperand,
                    throwOnAndFailure)) {
                // FLINK MODIFICATION END
                typeErrorCount++;
            }
        }

        boolean ret;
        switch (composition) {
            case AND:
                ret = typeErrorCount == 0;
                break;
            case OR:
                ret = typeErrorCount < allowedRules.size();
                break;
            default:
                // should never come here
                throw Util.unexpected(composition);
        }

        if (!ret && throwOnFailure) {
            // In the case of a composite OR, we want to throw an error
            // describing in more detail what the problem was, hence doing the
            // loop again.
            for (SqlSingleOperandTypeChecker rule : rules) {
                // FLINK MODIFICATION BEGIN
                rule.checkSingleOperandType(
                        callBinding,
                        node,
                        rule.getClass() == FamilyOperandTypeChecker.class ? 0 : iFormalOperand,
                        true);
                // FLINK MODIFICATION END
            }

            // If no exception thrown, just throw a generic validation signature
            // error.
            throw callBinding.newValidationSignatureError();
        }

        return ret;
    }
}
