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
package org.apache.calcite.rel.rules;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

// This class is copied from Apache Calcite except that it does not
// automatically name the field using the name of the operators
// as the Table API rejects special characters like '-' in the field names.

/**
 * PushProjector is a utility class used to perform operations used in push
 * projection rules.
 *
 * <p>Pushing is particularly interesting in the case of join, because there
 * are multiple inputs. Generally an expression can be pushed down to a
 * particular input if it depends upon no other inputs. If it can be pushed
 * down to both sides, it is pushed down to the left.
 *
 * <p>Sometimes an expression needs to be split before it can be pushed down.
 * To flag that an expression cannot be split, specify a rule that it must be
 * <dfn>preserved</dfn>. Such an expression will be pushed down intact to one
 * of the inputs, or not pushed down at all.</p>
 */
public class PushProjector {
  //~ Instance fields --------------------------------------------------------

  private final Project origProj;
  private final RexNode origFilter;
  private final RelNode childRel;
  private final ExprCondition preserveExprCondition;
  private final RelBuilder relBuilder;

  /**
   * Original projection expressions
   */
  final List<RexNode> origProjExprs;

  /**
   * Fields from the RelNode that the projection is being pushed past
   */
  final List<RelDataTypeField> childFields;

  /**
   * Number of fields in the RelNode that the projection is being pushed past
   */
  final int nChildFields;

  /**
   * Bitmap containing the references in the original projection
   */
  final BitSet projRefs;

  /**
   * Bitmap containing the fields in the RelNode that the projection is being
   * pushed past, if the RelNode is not a join. If the RelNode is a join, then
   * the fields correspond to the left hand side of the join.
   */
  final ImmutableBitSet childBitmap;

  /**
   * Bitmap containing the fields in the right hand side of a join, in the
   * case where the projection is being pushed past a join. Not used
   * otherwise.
   */
  final ImmutableBitSet rightBitmap;

  /**
   * Bitmap containing the fields that should be strong, i.e. when preserving expressions
   * we can only preserve them if the expressions if it is null when these fields are null.
   */
  final ImmutableBitSet strongBitmap;

  /**
   * Number of fields in the RelNode that the projection is being pushed past,
   * if the RelNode is not a join. If the RelNode is a join, then this is the
   * number of fields in the left hand side of the join.
   *
   * <p>The identity
   * {@code nChildFields == nSysFields + nFields + nFieldsRight}
   * holds. {@code nFields} does not include {@code nSysFields}.
   * The output of a join looks like this:
   *
   * <blockquote><pre>
   * | nSysFields | nFields | nFieldsRight |
   * </pre></blockquote>
   *
   * <p>The output of a single-input rel looks like this:
   *
   * <blockquote><pre>
   * | nSysFields | nFields |
   * </pre></blockquote>
   */
  final int nFields;

  /**
   * Number of fields in the right hand side of a join, in the case where the
   * projection is being pushed past a join. Always 0 otherwise.
   */
  final int nFieldsRight;

  /**
   * Number of system fields. System fields appear at the start of a join,
   * before the first field from the left input.
   */
  private final int nSysFields;

  /**
   * Expressions referenced in the projection/filter that should be preserved.
   * In the case where the projection is being pushed past a join, then the
   * list only contains the expressions corresponding to the left hand side of
   * the join.
   */
  final List<RexNode> childPreserveExprs;

  /**
   * Expressions referenced in the projection/filter that should be preserved,
   * corresponding to expressions on the right hand side of the join, if the
   * projection is being pushed past a join. Empty list otherwise.
   */
  final List<RexNode> rightPreserveExprs;

  /**
   * Number of system fields being projected.
   */
  int nSystemProject;

  /**
   * Number of fields being projected. In the case where the projection is
   * being pushed past a join, the number of fields being projected from the
   * left hand side of the join.
   */
  int nProject;

  /**
   * Number of fields being projected from the right hand side of a join, in
   * the case where the projection is being pushed past a join. 0 otherwise.
   */
  int nRightProject;

  /**
   * Rex builder used to create new expressions.
   */
  final RexBuilder rexBuilder;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushProjector object for pushing projects past a RelNode.
   *
   * @param origProj              the original projection that is being pushed;
   *                              may be null if the projection is implied as a
   *                              result of a projection having been trivially
   *                              removed
   * @param origFilter            the filter that the projection must also be
   *                              pushed past, if applicable
   * @param childRel              the RelNode that the projection is being
   *                              pushed past
   * @param preserveExprCondition condition for whether an expression should
   *                              be preserved in the projection
   */
  public PushProjector(
      Project origProj,
      RexNode origFilter,
      RelNode childRel,
      ExprCondition preserveExprCondition,
      RelBuilder relBuilder) {
    this.origProj = origProj;
    this.origFilter = origFilter;
    this.childRel = childRel;
    this.preserveExprCondition = preserveExprCondition;
    this.relBuilder = Preconditions.checkNotNull(relBuilder);
    if (origProj == null) {
      origProjExprs = ImmutableList.of();
    } else {
      origProjExprs = origProj.getProjects();
    }

    childFields = childRel.getRowType().getFieldList();
    nChildFields = childFields.size();

    projRefs = new BitSet(nChildFields);
    if (childRel instanceof Join) {
      Join joinRel = (Join) childRel;
      List<RelDataTypeField> leftFields =
          joinRel.getLeft().getRowType().getFieldList();
      List<RelDataTypeField> rightFields =
          joinRel.getRight().getRowType().getFieldList();
      nFields = leftFields.size();
      nFieldsRight = childRel instanceof SemiJoin ? 0 : rightFields.size();
      nSysFields = joinRel.getSystemFieldList().size();
      childBitmap =
          ImmutableBitSet.range(nSysFields, nFields + nSysFields);
      rightBitmap =
          ImmutableBitSet.range(nFields + nSysFields, nChildFields);

      switch (joinRel.getJoinType()) {
      case INNER:
        strongBitmap = ImmutableBitSet.of();
        break;
      case RIGHT:  // All the left-input's columns must be strong
        strongBitmap = ImmutableBitSet.range(nSysFields, nFields + nSysFields);
        break;
      case LEFT: // All the right-input's columns must be strong
        strongBitmap = ImmutableBitSet.range(nFields + nSysFields, nChildFields);
        break;
      case FULL:
      default:
        strongBitmap = ImmutableBitSet.range(nSysFields, nChildFields);
      }

    } else {
      nFields = nChildFields;
      nFieldsRight = 0;
      childBitmap = ImmutableBitSet.range(nChildFields);
      rightBitmap = null;
      nSysFields = 0;
      strongBitmap = ImmutableBitSet.of();
    }
    assert nChildFields == nSysFields + nFields + nFieldsRight;

    childPreserveExprs = new ArrayList<RexNode>();
    rightPreserveExprs = new ArrayList<RexNode>();

    rexBuilder = childRel.getCluster().getRexBuilder();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Decomposes a projection to the input references referenced by a
   * projection and a filter, either of which is optional. If both are
   * provided, the filter is underneath the project.
   *
   * <p>Creates a projection containing all input references as well as
   * preserving any special expressions. Converts the original projection
   * and/or filter to reference the new projection. Then, finally puts on top,
   * a final projection corresponding to the original projection.
   *
   * @param defaultExpr expression to be used in the projection if no fields
   *                    or special columns are selected
   * @return the converted projection if it makes sense to push elements of
   * the projection; otherwise returns null
   */
  public RelNode convertProject(RexNode defaultExpr) {
    // locate all fields referenced in the projection and filter
    locateAllRefs();

    // if all columns are being selected (either explicitly in the
    // projection) or via a "select *", then there needs to be some
    // special expressions to preserve in the projection; otherwise,
    // there's no point in proceeding any further
    if (origProj == null) {
      if (childPreserveExprs.size() == 0) {
        return null;
      }

      // even though there is no projection, this is the same as
      // selecting all fields
      if (nChildFields > 0) {
        // Calling with nChildFields == 0 should be safe but hits
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
        projRefs.set(0, nChildFields);
      }
      nProject = nChildFields;
    } else if (
        (projRefs.cardinality() == nChildFields)
            && (childPreserveExprs.size() == 0)) {
      return null;
    }

    // if nothing is being selected from the underlying rel, just
    // project the default expression passed in as a parameter or the
    // first column if there is no default expression
    if ((projRefs.cardinality() == 0) && (childPreserveExprs.size() == 0)) {
      if (defaultExpr != null) {
        childPreserveExprs.add(defaultExpr);
      } else if (nChildFields == 1) {
        return null;
      } else {
        projRefs.set(0);
        nProject = 1;
      }
    }

    // create a new projection referencing all fields referenced in
    // either the project or the filter
    RelNode newProject = createProjectRefsAndExprs(childRel, false, false);

    int[] adjustments = getAdjustments();

    // if a filter was passed in, convert it to reference the projected
    // columns, placing it on top of the project just created
    RelNode projChild;
    if (origFilter != null) {
      RexNode newFilter =
          convertRefsAndExprs(
              origFilter,
              newProject.getRowType().getFieldList(),
              adjustments);
      relBuilder.push(newProject);
      relBuilder.filter(newFilter);
      projChild = relBuilder.build();
    } else {
      projChild = newProject;
    }

    // put the original project on top of the filter/project, converting
    // it to reference the modified projection list; otherwise, create
    // a projection that essentially selects all fields
    return createNewProject(projChild, adjustments);
  }

  /**
   * Locates all references found in either the projection expressions a
   * filter, as well as references to expressions that should be preserved.
   * Based on that, determines whether pushing the projection makes sense.
   *
   * @return true if all inputs from the child that the projection is being
   * pushed past are referenced in the projection/filter and no special
   * preserve expressions are referenced; in that case, it does not make sense
   * to push the projection
   */
  public boolean locateAllRefs() {
    RexUtil.apply(
        new InputSpecialOpFinder(
            projRefs,
            childBitmap,
            rightBitmap,
            strongBitmap,
            preserveExprCondition,
            childPreserveExprs,
            rightPreserveExprs),
        origProjExprs,
        origFilter);

    // The system fields of each child are always used by the join, even if
    // they are not projected out of it.
    projRefs.set(
        nSysFields,
        nSysFields + nSysFields,
        true);
    projRefs.set(
        nSysFields + nFields,
        nSysFields + nFields + nSysFields,
        true);

    // Count how many fields are projected.
    nSystemProject = 0;
    nProject = 0;
    nRightProject = 0;
    for (int bit : BitSets.toIter(projRefs)) {
      if (bit < nSysFields) {
        nSystemProject++;
      } else if (bit < nSysFields + nFields) {
        nProject++;
      } else {
        nRightProject++;
      }
    }

    assert nSystemProject + nProject + nRightProject
        == projRefs.cardinality();

    if ((childRel instanceof Join)
        || (childRel instanceof SetOp)) {
      // if nothing is projected from the children, arbitrarily project
      // the first columns; this is necessary since Fennel doesn't
      // handle 0-column projections
      if ((nProject == 0) && (childPreserveExprs.size() == 0)) {
        projRefs.set(0);
        nProject = 1;
      }
      if (childRel instanceof Join) {
        if ((nRightProject == 0) && (rightPreserveExprs.size() == 0)) {
          projRefs.set(nFields);
          nRightProject = 1;
        }
      }
    }

    // no need to push projections if all children fields are being
    // referenced and there are no special preserve expressions; note
    // that we need to do this check after we've handled the 0-column
    // project cases
    if (projRefs.cardinality() == nChildFields
        && childPreserveExprs.size() == 0
        && rightPreserveExprs.size() == 0) {
      return true;
    }

    return false;
  }

  /**
   * Creates a projection based on the inputs specified in a bitmap and the
   * expressions that need to be preserved. The expressions are appended after
   * the input references.
   *
   * @param projChild child that the projection will be created on top of
   * @param adjust    if true, need to create new projection expressions;
   *                  otherwise, the existing ones are reused
   * @param rightSide if true, creating a projection for the right hand side
   *                  of a join
   * @return created projection
   */
  public Project createProjectRefsAndExprs(
      RelNode projChild,
      boolean adjust,
      boolean rightSide) {
    List<RexNode> preserveExprs;
    int nInputRefs;
    int offset;

    if (rightSide) {
      preserveExprs = rightPreserveExprs;
      nInputRefs = nRightProject;
      offset = nSysFields + nFields;
    } else {
      preserveExprs = childPreserveExprs;
      nInputRefs = nProject;
      offset = nSysFields;
    }
    int refIdx = offset - 1;
    List<Pair<RexNode, String>> newProjects =
        new ArrayList<Pair<RexNode, String>>();
    List<RelDataTypeField> destFields =
        projChild.getRowType().getFieldList();

    // add on the input references
    for (int i = 0; i < nInputRefs; i++) {
      refIdx = projRefs.nextSetBit(refIdx + 1);
      assert refIdx >= 0;
      final RelDataTypeField destField = destFields.get(refIdx - offset);
      newProjects.add(
          Pair.of(
              (RexNode) rexBuilder.makeInputRef(
                  destField.getType(), refIdx - offset),
              destField.getName()));
    }

    // add on the expressions that need to be preserved, converting the
    // arguments to reference the projected columns (if necessary)
    int[] adjustments = {};
    if ((preserveExprs.size() > 0) && adjust) {
      adjustments = new int[childFields.size()];
      for (int idx = offset; idx < childFields.size(); idx++) {
        adjustments[idx] = -offset;
      }
    }
    for (RexNode projExpr : preserveExprs) {
      RexNode newExpr;
      if (adjust) {
        newExpr =
            projExpr.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    childFields,
                    destFields,
                    adjustments));
      } else {
        newExpr = projExpr;
      }
      newProjects.add(
		 Pair.of(
              newExpr,
              null));
    }

    return (Project) RelOptUtil.createProject(
        projChild,
        Pair.left(newProjects),
        Pair.right(newProjects),
        false,
        relBuilder);
  }

  /**
   * Determines how much each input reference needs to be adjusted as a result
   * of projection
   *
   * @return array indicating how much each input needs to be adjusted by
   */
  public int[] getAdjustments() {
    int[] adjustments = new int[nChildFields];
    int newIdx = 0;
    int rightOffset = childPreserveExprs.size();
    for (int pos : BitSets.toIter(projRefs)) {
      adjustments[pos] = -(pos - newIdx);
      if (pos >= nSysFields + nFields) {
        adjustments[pos] += rightOffset;
      }
      newIdx++;
    }
    return adjustments;
  }

  /**
   * Clones an expression tree and walks through it, adjusting each
   * RexInputRef index by some amount, and converting expressions that need to
   * be preserved to field references.
   *
   * @param rex         the expression
   * @param destFields  fields that the new expressions will be referencing
   * @param adjustments the amount each input reference index needs to be
   *                    adjusted by
   * @return modified expression tree
   */
  public RexNode convertRefsAndExprs(
      RexNode rex,
      List<RelDataTypeField> destFields,
      int[] adjustments) {
    return rex.accept(
        new RefAndExprConverter(
            rexBuilder,
            childFields,
            destFields,
            adjustments,
            childPreserveExprs,
            nProject,
            rightPreserveExprs,
            nProject + childPreserveExprs.size() + nRightProject));
  }

  /**
   * Creates a new projection based on the original projection, adjusting all
   * input refs using an adjustment array passed in. If there was no original
   * projection, create a new one that selects every field from the underlying
   * rel.
   *
   * <p>If the resulting projection would be trivial, return the child.
   *
   * @param projChild   child of the new project
   * @param adjustments array indicating how much each input reference should
   *                    be adjusted by
   * @return the created projection
   */
  public RelNode createNewProject(RelNode projChild, int[] adjustments) {
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    if (origProj != null) {
      for (Pair<RexNode, String> p : origProj.getNamedProjects()) {
        projects.add(
            Pair.of(
                convertRefsAndExprs(
                    p.left,
                    projChild.getRowType().getFieldList(),
                    adjustments),
                p.right));
      }
    } else {
      for (Ord<RelDataTypeField> field : Ord.zip(childFields)) {
        projects.add(
            Pair.of(
                (RexNode) rexBuilder.makeInputRef(
                    field.e.getType(), field.i), field.e.getName()));
      }
    }
    return RelOptUtil.createProject(
        projChild,
        Pair.left(projects),
        Pair.right(projects),
        true /* optimize to avoid trivial projections, as per javadoc */,
        relBuilder);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Visitor which builds a bitmap of the inputs used by an expressions, as
   * well as locating expressions corresponding to special operators.
   */
  private class InputSpecialOpFinder extends RexVisitorImpl<Void> {
    private final BitSet rexRefs;
    private final ImmutableBitSet leftFields;
    private final ImmutableBitSet rightFields;
    private final ImmutableBitSet strongFields;
    private final ExprCondition preserveExprCondition;
    private final List<RexNode> preserveLeft;
    private final List<RexNode> preserveRight;
    private final Strong strong;

    public InputSpecialOpFinder(
        BitSet rexRefs,
        ImmutableBitSet leftFields,
        ImmutableBitSet rightFields,
        final ImmutableBitSet strongFields,
        ExprCondition preserveExprCondition,
        List<RexNode> preserveLeft,
        List<RexNode> preserveRight) {
      super(true);
      this.rexRefs = rexRefs;
      this.leftFields = leftFields;
      this.rightFields = rightFields;
      this.preserveExprCondition = preserveExprCondition;
      this.preserveLeft = preserveLeft;
      this.preserveRight = preserveRight;

      this.strongFields = strongFields;
      this.strong = Strong.of(strongFields);
    }

    public Void visitCall(RexCall call) {
      if (preserve(call)) {
        return null;
      }
      super.visitCall(call);
      return null;
    }

    private boolean isStrong(final ImmutableBitSet exprArgs, final RexNode call) {
      // If the expressions do not use any of the inputs that require output to be null,
      // no need to check.  Otherwise, check that the expression is null.
      // For example, in an "left outer join", we don't require that expressions
      // pushed down into the left input to be strong.  On the other hand,
      // expressions pushed into the right input must be.  In that case,
      // strongFields == right input fields.
      return !strongFields.intersects(exprArgs) || strong.isNull(call);
    }

    private boolean preserve(RexNode call) {
      if (preserveExprCondition.test(call)) {
        // if the arguments of the expression only reference the
        // left hand side, preserve it on the left; similarly, if
        // it only references expressions on the right
        final ImmutableBitSet exprArgs = RelOptUtil.InputFinder.bits(call);
        if (exprArgs.cardinality() > 0) {
          if (leftFields.contains(exprArgs) && isStrong(exprArgs, call)) {
            addExpr(preserveLeft, call);
            return true;
          } else if (rightFields.contains(exprArgs) && isStrong(exprArgs, call)) {
            assert preserveRight != null;
            addExpr(preserveRight, call);
            return true;
          }
        }
        // if the expression arguments reference both the left and
        // right, fall through and don't attempt to preserve the
        // expression, but instead locate references and special
        // ops in the call operands
      }
      return false;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      rexRefs.set(inputRef.getIndex());
      return null;
    }

    /**
     * Adds an expression to a list if the same expression isn't already in
     * the list. Expressions are identical if their digests are the same.
     *
     * @param exprList current list of expressions
     * @param newExpr  new expression to be added
     */
    private void addExpr(List<RexNode> exprList, RexNode newExpr) {
      String newExprString = newExpr.toString();
      for (RexNode expr : exprList) {
        if (newExprString.compareTo(expr.toString()) == 0) {
          return;
        }
      }
      exprList.add(newExpr);
    }
  }

  /**
   * Walks an expression tree, replacing input refs with new values to reflect
   * projection and converting special expressions to field references.
   */
  private class RefAndExprConverter extends RelOptUtil.RexInputConverter {
    private final List<RexNode> preserveLeft;
    private final int firstLeftRef;
    private final List<RexNode> preserveRight;
    private final int firstRightRef;

    public RefAndExprConverter(
        RexBuilder rexBuilder,
        List<RelDataTypeField> srcFields,
        List<RelDataTypeField> destFields,
        int[] adjustments,
        List<RexNode> preserveLeft,
        int firstLeftRef,
        List<RexNode> preserveRight,
        int firstRightRef) {
      super(rexBuilder, srcFields, destFields, adjustments);
      this.preserveLeft = preserveLeft;
      this.firstLeftRef = firstLeftRef;
      this.preserveRight = preserveRight;
      this.firstRightRef = firstRightRef;
    }

    public RexNode visitCall(RexCall call) {
      // if the expression corresponds to one that needs to be preserved,
      // convert it to a field reference; otherwise, convert the entire
      // expression
      int match =
          findExprInLists(
              call,
              preserveLeft,
              firstLeftRef,
              preserveRight,
              firstRightRef);
      if (match >= 0) {
        return rexBuilder.makeInputRef(
            destFields.get(match).getType(),
            match);
      }
      return super.visitCall(call);
    }

    /**
     * Looks for a matching RexNode from among two lists of RexNodes and
     * returns the offset into the list corresponding to the match, adjusted
     * by an amount, depending on whether the match was from the first or
     * second list.
     *
     * @param rex      RexNode that is being matched against
     * @param rexList1 first list of RexNodes
     * @param adjust1  adjustment if match occurred in first list
     * @param rexList2 second list of RexNodes
     * @param adjust2  adjustment if match occurred in the second list
     * @return index in the list corresponding to the matching RexNode; -1
     * if no match
     */
    private int findExprInLists(
        RexNode rex,
        List<RexNode> rexList1,
        int adjust1,
        List<RexNode> rexList2,
        int adjust2) {
      int match = findExprInList(rex, rexList1);
      if (match >= 0) {
        return match + adjust1;
      }

      if (rexList2 != null) {
        match = findExprInList(rex, rexList2);
        if (match >= 0) {
          return match + adjust2;
        }
      }

      return -1;
    }

    private int findExprInList(RexNode rex, List<RexNode> rexList) {
      int match = 0;
      for (RexNode rexElement : rexList) {
        if (rexElement.toString().compareTo(rex.toString()) == 0) {
          return match;
        }
        match++;
      }
      return -1;
    }
  }

  /**
   * A functor that replies true or false for a given expression.
   *
   * @see org.apache.calcite.rel.rules.PushProjector.OperatorExprCondition
   */
  public interface ExprCondition extends Predicate<RexNode> {
    /**
     * Evaluates a condition for a given expression.
     *
     * @param expr Expression
     * @return result of evaluating the condition
     */
    boolean test(RexNode expr);

    /**
     * Constant condition that replies {@code false} for all expressions.
     */
    ExprCondition FALSE =
        new ExprConditionImpl() {
          @Override public boolean test(RexNode expr) {
            return false;
          }
        };

    /**
     * Constant condition that replies {@code true} for all expressions.
     */
    ExprCondition TRUE =
        new ExprConditionImpl() {
          @Override public boolean test(RexNode expr) {
            return true;
          }
        };
  }

  /** Implementation of {@link ExprCondition}. */
  abstract static class ExprConditionImpl extends PredicateImpl<RexNode>
      implements ExprCondition {
  }

  /**
   * An expression condition that evaluates to true if the expression is
   * a call to one of a set of operators.
   */
  class OperatorExprCondition extends ExprConditionImpl {
    private final Set<SqlOperator> operatorSet;

    /**
     * Creates an OperatorExprCondition.
     *
     * @param operatorSet Set of operators
     */
    public OperatorExprCondition(Iterable<? extends SqlOperator> operatorSet) {
      this.operatorSet = ImmutableSet.copyOf(operatorSet);
    }

    public boolean test(RexNode expr) {
      return expr instanceof RexCall
          && operatorSet.contains(((RexCall) expr).getOperator());
    }
  }
}

// End PushProjector.java
