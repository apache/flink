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
package org.apache.calcite.rex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * A collection of expressions which read inputs, compute output expressions, and optionally use a
 * condition to filter rows.
 *
 * <p>Programs are immutable. It may help to use a {@link RexProgramBuilder}, which has the same
 * relationship to {@link RexProgram} as {@link StringBuilder} has to {@link String}.
 *
 * <p>A program can contain aggregate functions. If it does, the arguments to each aggregate
 * function must be an {@link RexInputRef}.
 *
 * <p>FLINK modifications (backport of CALCITE-6764): Lines 999 ~ 1002
 *
 * @see RexProgramBuilder
 */
public class RexProgram {
    // ~ Instance fields --------------------------------------------------------

    /**
     * First stage of expression evaluation. The expressions in this array can refer to inputs
     * (using input ordinal #0) or previous expressions in the array (using input ordinal #1).
     */
    private final List<RexNode> exprs;

    /** With {@link #condition}, the second stage of expression evaluation. */
    private final List<RexLocalRef> projects;

    /** The optional condition. If null, the calculator does not filter rows. */
    private final @Nullable RexLocalRef condition;

    private final RelDataType inputRowType;

    private final RelDataType outputRowType;

    /** Reference counts for each expression, computed on demand. */
    private int[] refCounts;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a program.
     *
     * <p>The expressions must be valid: they must not contain common expressions, forward
     * references, or non-trivial aggregates.
     *
     * @param inputRowType Input row type
     * @param exprs Common expressions
     * @param projects Projection expressions
     * @param condition Condition expression. If null, calculator does not filter rows
     * @param outputRowType Description of the row produced by the program
     */
    public RexProgram(
            RelDataType inputRowType,
            List<? extends RexNode> exprs,
            List<RexLocalRef> projects,
            @Nullable RexLocalRef condition,
            RelDataType outputRowType) {
        this.inputRowType = inputRowType;
        this.exprs = ImmutableList.copyOf(exprs);
        this.projects = ImmutableList.copyOf(projects);
        this.condition = condition;
        this.outputRowType = outputRowType;
        assert isValid(Litmus.THROW, null);
    }

    // ~ Methods ----------------------------------------------------------------

    // REVIEW jvs 16-Oct-2006:  The description below is confusing.  I
    // think it means "none of the entries are null, there may be none,
    // and there is no further reduction into smaller common sub-expressions
    // possible"?

    /**
     * Returns the common sub-expressions of this program.
     *
     * <p>The list is never null but may be empty; each the expression in the list is not null; and
     * no further reduction into smaller common sub-expressions is possible.
     */
    public List<RexNode> getExprList() {
        return exprs;
    }

    /**
     * Returns an array of references to the expressions which this program is to project. Never
     * null, may be empty.
     */
    public List<RexLocalRef> getProjectList() {
        return projects;
    }

    /** Returns a list of project expressions and their field names. */
    public List<Pair<RexLocalRef, String>> getNamedProjects() {
        return new AbstractList<Pair<RexLocalRef, String>>() {
            @Override
            public int size() {
                return projects.size();
            }

            @Override
            public Pair<RexLocalRef, String> get(int index) {
                return Pair.of(
                        projects.get(index), outputRowType.getFieldList().get(index).getName());
            }
        };
    }

    /**
     * Returns the field reference of this program's filter condition, or null if there is no
     * condition.
     */
    public @Nullable RexLocalRef getCondition() {
        return condition;
    }

    /**
     * Creates a program which calculates projections and filters rows based upon a condition. Does
     * not attempt to eliminate common sub-expressions.
     *
     * @param projectExprs Project expressions
     * @param conditionExpr Condition on which to filter rows, or null if rows are not to be
     *     filtered
     * @param outputRowType Output row type
     * @param rexBuilder Builder of rex expressions
     * @return A program
     */
    public static RexProgram create(
            RelDataType inputRowType,
            List<? extends RexNode> projectExprs,
            @Nullable RexNode conditionExpr,
            RelDataType outputRowType,
            RexBuilder rexBuilder) {
        return create(
                inputRowType,
                projectExprs,
                conditionExpr,
                outputRowType.getFieldNames(),
                rexBuilder);
    }

    /**
     * Creates a program which calculates projections and filters rows based upon a condition. Does
     * not attempt to eliminate common sub-expressions.
     *
     * @param projectExprs Project expressions
     * @param conditionExpr Condition on which to filter rows, or null if rows are not to be
     *     filtered
     * @param fieldNames Names of projected fields
     * @param rexBuilder Builder of rex expressions
     * @return A program
     */
    public static RexProgram create(
            RelDataType inputRowType,
            List<? extends RexNode> projectExprs,
            @Nullable RexNode conditionExpr,
            @Nullable List<? extends @Nullable String> fieldNames,
            RexBuilder rexBuilder) {
        if (fieldNames == null) {
            fieldNames = Collections.nCopies(projectExprs.size(), null);
        } else {
            assert fieldNames.size() == projectExprs.size()
                    : "fieldNames=" + fieldNames + ", exprs=" + projectExprs;
        }
        final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        for (int i = 0; i < projectExprs.size(); i++) {
            programBuilder.addProject(projectExprs.get(i), fieldNames.get(i));
        }
        if (conditionExpr != null) {
            programBuilder.addCondition(conditionExpr);
        }
        return programBuilder.getProgram();
    }

    /**
     * Create a program from serialized output. In this case, the input is mainly from the output
     * json string of {@link RelJsonWriter}
     */
    public static RexProgram create(RelInput input) {
        final List<RexNode> exprs = requireNonNull(input.getExpressionList("exprs"), "exprs");
        final List<RexNode> projectRexNodes =
                requireNonNull(input.getExpressionList("projects"), "projects");
        final List<RexLocalRef> projects = new ArrayList<>(projectRexNodes.size());
        for (RexNode rexNode : projectRexNodes) {
            projects.add((RexLocalRef) rexNode);
        }
        final RelDataType inputType = input.getRowType("inputRowType");
        final RelDataType outputType = input.getRowType("outputRowType");
        final RexLocalRef condition = (RexLocalRef) input.getExpression("condition");
        return new RexProgram(inputType, exprs, projects, condition, outputType);
    }

    // description of this calc, chiefly intended for debugging
    @Override
    public String toString() {
        // Intended to produce similar output to explainCalc,
        // but without requiring a RelNode or RelOptPlanWriter.
        final RelWriterImpl pw = new RelWriterImpl(new PrintWriter(new StringWriter()));
        collectExplainTerms("", pw);
        return pw.simple();
    }

    /**
     * Writes an explanation of the expressions in this program to a plan writer.
     *
     * @param pw Plan writer
     */
    public RelWriter explainCalc(RelWriter pw) {
        if (pw instanceof RelJsonWriter) {
            return pw.item("exprs", exprs)
                    .item("projects", projects)
                    .item("condition", condition)
                    .item("inputRowType", inputRowType)
                    .item("outputRowType", outputRowType);
        } else {
            return collectExplainTerms("", pw, pw.getDetailLevel());
        }
    }

    public RelWriter collectExplainTerms(String prefix, RelWriter pw) {
        return collectExplainTerms(prefix, pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    /**
     * Collects the expressions in this program into a list of terms and values.
     *
     * @param prefix Prefix for term names, usually the empty string, but useful if a relational
     *     expression contains more than one program
     * @param pw Plan writer
     */
    public RelWriter collectExplainTerms(String prefix, RelWriter pw, SqlExplainLevel level) {
        final List<RelDataTypeField> inFields = inputRowType.getFieldList();
        final List<RelDataTypeField> outFields = outputRowType.getFieldList();
        assert outFields.size() == projects.size()
                : "outFields.length=" + outFields.size() + ", projects.length=" + projects.size();
        pw.item(
                prefix + "expr#0" + ((inFields.size() > 1) ? (".." + (inFields.size() - 1)) : ""),
                "{inputs}");
        for (int i = inFields.size(); i < exprs.size(); i++) {
            pw.item(prefix + "expr#" + i, exprs.get(i));
        }

        // If a lot of the fields are simply projections of the underlying
        // expression, try to be a bit less verbose.
        int trivialCount = countTrivial(projects);

        switch (trivialCount) {
            case 0:
                break;
            case 1:
                trivialCount = 0;
                break;
            default:
                pw.item(prefix + "proj#0.." + (trivialCount - 1), "{exprs}");
                break;
        }

        final boolean withFieldNames = level != SqlExplainLevel.DIGEST_ATTRIBUTES;
        // Print the non-trivial fields with their names as they appear in the
        // output row type.
        for (int i = trivialCount; i < projects.size(); i++) {
            final String fieldName =
                    withFieldNames ? prefix + outFields.get(i).getName() : prefix + i;
            pw.item(fieldName, projects.get(i));
        }
        if (condition != null) {
            pw.item(prefix + "$condition", condition);
        }
        return pw;
    }

    /**
     * Returns the number of expressions at the front of an array which are simply projections of
     * the same field.
     *
     * @param refs References
     */
    private static int countTrivial(List<RexLocalRef> refs) {
        for (int i = 0; i < refs.size(); i++) {
            RexLocalRef ref = refs.get(i);
            if (ref.getIndex() != i) {
                return i;
            }
        }
        return refs.size();
    }

    /** Returns the number of expressions in this program. */
    public int getExprCount() {
        return exprs.size() + projects.size() + ((condition == null) ? 0 : 1);
    }

    /** Creates the identity program. */
    public static RexProgram createIdentity(RelDataType rowType) {
        return createIdentity(rowType, rowType);
    }

    /**
     * Creates a program that projects its input fields but with possibly different names for the
     * output fields.
     */
    public static RexProgram createIdentity(RelDataType rowType, RelDataType outputRowType) {
        if (rowType != outputRowType
                && !Pair.right(rowType.getFieldList())
                        .equals(Pair.right(outputRowType.getFieldList()))) {
            throw new IllegalArgumentException(
                    "field type mismatch: " + rowType + " vs. " + outputRowType);
        }
        final List<RelDataTypeField> fields = rowType.getFieldList();
        final List<RexLocalRef> projectRefs = new ArrayList<>();
        final List<RexInputRef> refs = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            final RexInputRef ref = RexInputRef.of(i, fields);
            refs.add(ref);
            projectRefs.add(new RexLocalRef(i, ref.getType()));
        }
        return new RexProgram(rowType, refs, projectRefs, null, outputRowType);
    }

    /**
     * Returns the type of the input row to the program.
     *
     * @return input row type
     */
    public RelDataType getInputRowType() {
        return inputRowType;
    }

    /**
     * Returns whether this program contains windowed aggregate functions.
     *
     * @return whether this program contains windowed aggregate functions
     */
    public boolean containsAggs() {
        return RexOver.containsOver(this);
    }

    /**
     * Returns the type of the output row from this program.
     *
     * @return output row type
     */
    public RelDataType getOutputRowType() {
        return outputRowType;
    }

    /**
     * Checks that this program is valid.
     *
     * <p>If <code>fail</code> is true, executes <code>assert false</code>, so will throw an {@link
     * AssertionError} if assertions are enabled. If <code>
     * fail</code> is false, merely returns whether the program is valid.
     *
     * @param litmus What to do if an error is detected
     * @param context Context of enclosing {@link RelNode}, for validity checking, or null if not
     *     known
     * @return Whether the program is valid
     */
    public boolean isValid(Litmus litmus, RelNode.Context context) {
        if (inputRowType == null) {
            return litmus.fail(null);
        }
        if (exprs == null) {
            return litmus.fail(null);
        }
        if (projects == null) {
            return litmus.fail(null);
        }
        if (outputRowType == null) {
            return litmus.fail(null);
        }

        // If the input row type is a struct (contains fields) then the leading
        // expressions must be references to those fields. But we don't require
        // this if the input row type is, say, a java class.
        if (inputRowType.isStruct()) {
            if (!RexUtil.containIdentity(exprs, inputRowType, litmus)) {
                return litmus.fail(null);
            }

            // None of the other fields should be inputRefs.
            for (int i = inputRowType.getFieldCount(); i < exprs.size(); i++) {
                RexNode expr = exprs.get(i);
                if (expr instanceof RexInputRef) {
                    return litmus.fail(null);
                }
            }
        }
        // todo: enable
        // CHECKSTYLE: IGNORE 1
        if (false && RexUtil.containNoCommonExprs(exprs, litmus)) {
            return litmus.fail(null);
        }
        if (!RexUtil.containNoForwardRefs(exprs, inputRowType, litmus)) {
            return litmus.fail(null);
        }
        if (!RexUtil.containNoNonTrivialAggs(exprs, litmus)) {
            return litmus.fail(null);
        }
        final Checker checker = new Checker(inputRowType, RexUtil.types(exprs), null, litmus);
        if (condition != null) {
            if (!SqlTypeUtil.inBooleanFamily(condition.getType())) {
                return litmus.fail("condition must be boolean");
            }
            condition.accept(checker);
            if (checker.failCount > 0) {
                return litmus.fail(null);
            }
        }
        for (RexLocalRef project : projects) {
            project.accept(checker);
            if (checker.failCount > 0) {
                return litmus.fail(null);
            }
        }
        for (RexNode expr : exprs) {
            expr.accept(checker);
            if (checker.failCount > 0) {
                return litmus.fail(null);
            }
        }
        return litmus.succeed();
    }

    /**
     * Returns whether an expression always evaluates to null.
     *
     * <p>Like {@link RexUtil#isNull(RexNode)}, null literals are null, and casts of null literals
     * are null. But this method also regards references to null expressions as null.
     *
     * @param expr Expression
     * @return Whether expression always evaluates to null
     */
    public boolean isNull(RexNode expr) {
        switch (expr.getKind()) {
            case LITERAL:
                return ((RexLiteral) expr).getValue2() == null;
            case LOCAL_REF:
                RexLocalRef inputRef = (RexLocalRef) expr;
                return isNull(exprs.get(inputRef.index));
            case CAST:
                return isNull(((RexCall) expr).operands.get(0));
            default:
                return false;
        }
    }

    /**
     * Fully expands a RexLocalRef back into a pure RexNode tree containing no RexLocalRefs
     * (reversing the effect of common subexpression elimination). For example, <code>
     * program.expandLocalRef(program.getCondition())</code> will return the expansion of a
     * program's condition.
     *
     * @param ref a RexLocalRef from this program
     * @return expanded form
     */
    public RexNode expandLocalRef(RexLocalRef ref) {
        return ref.accept(new ExpansionShuttle(exprs));
    }

    /** Expands a list of expressions that may contain {@link RexLocalRef}s. */
    public List<RexNode> expandList(List<? extends RexNode> nodes) {
        return new ExpansionShuttle(exprs).visitList(nodes);
    }

    /**
     * Splits this program into a list of project expressions and a list of filter expressions.
     *
     * <p>Neither list is null. The filters are evaluated first.
     */
    public Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> split() {
        final List<RexNode> filters = new ArrayList<>();
        if (condition != null) {
            RelOptUtil.decomposeConjunction(expandLocalRef(condition), filters);
        }
        final ImmutableList.Builder<RexNode> projects = ImmutableList.builder();
        for (RexLocalRef project : this.projects) {
            projects.add(expandLocalRef(project));
        }
        return Pair.of(projects.build(), ImmutableList.copyOf(filters));
    }

    /**
     * Given a list of collations which hold for the input to this program, returns a list of
     * collations which hold for its output. The result is mutable and sorted.
     */
    public List<RelCollation> getCollations(List<RelCollation> inputCollations) {
        final List<RelCollation> outputCollations = new ArrayList<>();
        deduceCollations(outputCollations, inputRowType.getFieldCount(), projects, inputCollations);
        return outputCollations;
    }

    /**
     * Given a list of expressions and a description of which are ordered, populates a list of
     * collations, sorted in natural order.
     */
    public static void deduceCollations(
            List<RelCollation> outputCollations,
            final int sourceCount,
            List<RexLocalRef> refs,
            List<RelCollation> inputCollations) {
        int[] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            final int source = ref.getIndex();
            if ((source < sourceCount) && (targets[source] == -1)) {
                targets[source] = i;
            }
        }
        loop:
        for (RelCollation collation : inputCollations) {
            final List<RelFieldCollation> fieldCollations = new ArrayList<>(0);
            for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                final int source = fieldCollation.getFieldIndex();
                final int target = targets[source];
                if (target < 0) {
                    continue loop;
                }
                fieldCollations.add(fieldCollation.withFieldIndex(target));
            }

            // Success -- all of the source fields of this key are mapped
            // to the output.
            outputCollations.add(RelCollations.of(fieldCollations));
        }
        outputCollations.sort(Ordering.natural());
    }

    /**
     * Returns whether the fields on the leading edge of the project list are the input fields.
     *
     * @param fail Whether to throw an assert failure if does not project identity
     */
    public boolean projectsIdentity(final boolean fail) {
        final int fieldCount = inputRowType.getFieldCount();
        if (projects.size() < fieldCount) {
            assert !fail
                    : "program '"
                            + toString()
                            + "' does not project identity for input row type '"
                            + inputRowType
                            + "'";
            return false;
        }
        for (int i = 0; i < fieldCount; i++) {
            RexLocalRef project = projects.get(i);
            if (project.index != i) {
                assert !fail
                        : "program "
                                + toString()
                                + "' does not project identity for input row type '"
                                + inputRowType
                                + "', field #"
                                + i;
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether this program projects precisely its input fields. It may or may not apply a
     * condition.
     */
    public boolean projectsOnlyIdentity() {
        if (projects.size() != inputRowType.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < projects.size(); i++) {
            RexLocalRef project = projects.get(i);
            if (project.index != i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether this program returns its input exactly.
     *
     * <p>This is a stronger condition than {@link #projectsIdentity(boolean)}.
     */
    public boolean isTrivial() {
        return getCondition() == null && projectsOnlyIdentity();
    }

    /**
     * Gets reference counts for each expression in the program, where the references are detected
     * from later expressions in the same program, as well as the project list and condition.
     * Expressions with references counts greater than 1 are true common sub-expressions.
     *
     * @return array of reference counts; the ith element in the returned array is the number of
     *     references to getExprList()[i]
     */
    public int[] getReferenceCounts() {
        if (refCounts != null) {
            return refCounts;
        }
        refCounts = new int[exprs.size()];
        ReferenceCounter refCounter = new ReferenceCounter(refCounts);
        RexUtil.apply(refCounter, exprs, null);
        if (condition != null) {
            refCounter.visitLocalRef(condition);
        }
        for (RexLocalRef project : projects) {
            refCounter.visitLocalRef(project);
        }
        return refCounts;
    }

    /** Returns whether an expression is constant. */
    public boolean isConstant(RexNode ref) {
        return ref.accept(new ConstantFinder());
    }

    public @Nullable RexNode gatherExpr(RexNode expr) {
        return expr.accept(new Marshaller());
    }

    /**
     * Returns the input field that an output field is populated from, or -1 if it is populated from
     * an expression.
     */
    public int getSourceField(int outputOrdinal) {
        assert (outputOrdinal >= 0) && (outputOrdinal < this.projects.size());
        RexLocalRef project = projects.get(outputOrdinal);
        int index = project.index;
        while (true) {
            RexNode expr = exprs.get(index);
            if (expr instanceof RexCall
                    && ((RexCall) expr).getOperator() == SqlStdOperatorTable.IN_FENNEL) {
                // drill through identity function
                expr = ((RexCall) expr).getOperands().get(0);
            }
            if (expr instanceof RexLocalRef) {
                index = ((RexLocalRef) expr).index;
            } else if (expr instanceof RexInputRef) {
                return ((RexInputRef) expr).index;
            } else {
                return -1;
            }
        }
    }

    /** Returns whether this program is a permutation of its inputs. */
    public boolean isPermutation() {
        if (projects.size() != inputRowType.getFieldList().size()) {
            return false;
        }
        for (int i = 0; i < projects.size(); ++i) {
            if (getSourceField(i) < 0) {
                return false;
            }
        }
        return true;
    }

    /** Returns a permutation, if this program is a permutation, otherwise null. */
    public @Nullable Permutation getPermutation() {
        Permutation permutation = new Permutation(projects.size());
        if (projects.size() != inputRowType.getFieldList().size()) {
            return null;
        }
        for (int i = 0; i < projects.size(); ++i) {
            int sourceField = getSourceField(i);
            if (sourceField < 0) {
                return null;
            }
            permutation.set(i, sourceField);
        }
        return permutation;
    }

    /**
     * Returns the set of correlation variables used (read) by this program.
     *
     * @return set of correlation variable names
     */
    public Set<String> getCorrelVariableNames() {
        final Set<String> paramIdSet = new HashSet<>();
        RexUtil.apply(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
                        paramIdSet.add(correlVariable.getName());
                        return null;
                    }
                },
                exprs,
                null);
        return paramIdSet;
    }

    /**
     * Returns whether this program is in canonical form.
     *
     * @param litmus What to do if an error is detected (program is not in canonical form)
     * @param rexBuilder Rex builder
     * @return whether in canonical form
     */
    public boolean isNormalized(Litmus litmus, RexBuilder rexBuilder) {
        final RexProgram normalizedProgram = normalize(rexBuilder, null);
        String normalized = normalizedProgram.toString();
        String string = toString();
        if (!normalized.equals(string)) {
            final String message =
                    "Program is not normalized:\n" + "program:    {}\n" + "normalized: {}\n";
            return litmus.fail(message, string, normalized);
        }
        return litmus.succeed();
    }

    /**
     * Creates a simplified/normalized copy of this program.
     *
     * @param rexBuilder Rex builder
     * @param simplify Simplifier to simplify (in addition to normalizing), or null to not simplify
     * @return Normalized program
     */
    public RexProgram normalize(RexBuilder rexBuilder, @Nullable RexSimplify simplify) {
        // Normalize program by creating program builder from the program, then
        // converting to a program. getProgram does not need to normalize
        // because the builder was normalized on creation.
        assert isValid(Litmus.THROW, null);
        final RexProgramBuilder builder =
                RexProgramBuilder.create(
                        rexBuilder,
                        inputRowType,
                        exprs,
                        projects,
                        condition,
                        outputRowType,
                        true,
                        simplify);
        return builder.getProgram(false);
    }

    @Deprecated // to be removed before 2.0
    public RexProgram normalize(RexBuilder rexBuilder, boolean simplify) {
        final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
        return normalize(
                rexBuilder,
                simplify ? new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR) : null);
    }

    /**
     * Returns a partial mapping of a set of project expressions.
     *
     * <p>The mapping is an inverse function. Every target has a source field, but a source might
     * have 0, 1 or more targets. Project expressions that do not consist of a mapping are ignored.
     *
     * @param inputFieldCount Number of input fields
     * @return Mapping of a set of project expressions, never null
     */
    public Mappings.TargetMapping getPartialMapping(int inputFieldCount) {
        Mappings.TargetMapping mapping =
                Mappings.create(MappingType.INVERSE_FUNCTION, inputFieldCount, projects.size());
        for (Ord<RexLocalRef> exp : Ord.zip(projects)) {
            RexNode rexNode = expandLocalRef(exp.e);
            if (rexNode instanceof RexInputRef) {
                mapping.set(((RexInputRef) rexNode).getIndex(), exp.i);
            }
        }
        return mapping;
    }

    // ~ Inner Classes ----------------------------------------------------------

    /** Visitor which walks over a program and checks validity. */
    static class Checker extends RexChecker {
        private final List<RelDataType> internalExprTypeList;

        /**
         * Creates a Checker.
         *
         * @param inputRowType Types of the input fields
         * @param internalExprTypeList Types of the internal expressions
         * @param context Context of the enclosing {@link RelNode}, or null
         * @param litmus Whether to fail
         */
        Checker(
                RelDataType inputRowType,
                List<RelDataType> internalExprTypeList,
                RelNode.Context context,
                Litmus litmus) {
            super(inputRowType, context, litmus);
            this.internalExprTypeList = internalExprTypeList;
        }

        /**
         * Overrides {@link RexChecker} method, because {@link RexLocalRef} is is illegal in most
         * rex expressions, but legal in a program.
         */
        @Override
        public Boolean visitLocalRef(RexLocalRef localRef) {
            final int index = localRef.getIndex();
            if ((index < 0) || (index >= internalExprTypeList.size())) {
                ++failCount;
                return litmus.fail(null);
            }
            if (!RelOptUtil.eq(
                    "type1",
                    localRef.getType(),
                    "type2",
                    internalExprTypeList.get(index),
                    litmus)) {
                ++failCount;
                return litmus.fail(null);
            }
            return litmus.succeed();
        }
    }

    /** A RexShuttle used in the implementation of {@link RexProgram#expandLocalRef}. */
    static class ExpansionShuttle extends RexShuttle {
        private final List<RexNode> exprs;

        ExpansionShuttle(List<RexNode> exprs) {
            this.exprs = exprs;
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef localRef) {
            RexNode tree = exprs.get(localRef.getIndex());
            return tree.accept(this);
        }
    }

    /** Walks over an expression and determines whether it is constant. */
    private class ConstantFinder extends RexUtil.ConstantFinder {
        @Override
        public Boolean visitLocalRef(RexLocalRef localRef) {
            final RexNode expr = exprs.get(localRef.index);
            return expr.accept(this);
        }

        @Override
        public Boolean visitOver(RexOver over) {
            return false;
        }

        @Override
        public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
            // Correlating variables are constant WITHIN A RESTART, so that's
            // good enough.
            return true;
        }
    }

    /**
     * Given an expression in a program, creates a clone of the expression with sub-expressions
     * (represented by {@link RexLocalRef}s) fully expanded.
     */
    private class Marshaller extends RexVisitorImpl<@Nullable RexNode> {
        Marshaller() {
            super(false);
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return inputRef;
        }

        @Override
        public @Nullable RexNode visitLocalRef(RexLocalRef localRef) {
            final RexNode expr = exprs.get(localRef.index);
            return expr.accept(this);
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            final List<RexNode> newOperands = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                newOperands.add(castNonNull(operand.accept(this)));
            }
            return call.clone(call.getType(), newOperands);
        }

        @Override
        public RexNode visitOver(RexOver over) {
            return visitCall(over);
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        @Override
        public RexNode visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexNode referenceExpr = fieldAccess.getReferenceExpr().accept(this);
            return new RexFieldAccess(
                    requireNonNull(referenceExpr, "referenceExpr must not be null"),
                    fieldAccess.getField(),
                    fieldAccess.getType());
        }
    }

    /** Visitor which marks which expressions are used. */
    private static class ReferenceCounter extends RexVisitorImpl<Void> {
        private final int[] refCounts;

        ReferenceCounter(int[] refCounts) {
            super(true);
            this.refCounts = refCounts;
        }

        @Override
        public Void visitLocalRef(RexLocalRef localRef) {
            final int index = localRef.getIndex();
            refCounts[index]++;
            return null;
        }
    }
}
