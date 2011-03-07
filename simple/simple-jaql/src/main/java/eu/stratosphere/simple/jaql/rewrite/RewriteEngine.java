/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package eu.stratosphere.simple.jaql.rewrite;

import java.util.ArrayList;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.rewrite.AsArrayElimination;
import com.ibm.jaql.lang.rewrite.CheapConstEval;
import com.ibm.jaql.lang.rewrite.CombineInputSimplification;
import com.ibm.jaql.lang.rewrite.ConstArrayAccess;
import com.ibm.jaql.lang.rewrite.ConstFieldAccess;
import com.ibm.jaql.lang.rewrite.ConstIfElimination;
import com.ibm.jaql.lang.rewrite.ConstJumpElimination;
import com.ibm.jaql.lang.rewrite.DaisyChainInline;
import com.ibm.jaql.lang.rewrite.DiamondTagExpand;
import com.ibm.jaql.lang.rewrite.DoConstPragma;
import com.ibm.jaql.lang.rewrite.DoInlinePragma;
import com.ibm.jaql.lang.rewrite.DoMerge;
import com.ibm.jaql.lang.rewrite.DoPullup;
import com.ibm.jaql.lang.rewrite.EmptyOnNullElimination;
import com.ibm.jaql.lang.rewrite.FilterMerge;
import com.ibm.jaql.lang.rewrite.FilterPredicateSimplification;
import com.ibm.jaql.lang.rewrite.FilterPushDown;
import com.ibm.jaql.lang.rewrite.FlattenTagSplit;
import com.ibm.jaql.lang.rewrite.ForInSimpleIf;
import com.ibm.jaql.lang.rewrite.ForToLet;
import com.ibm.jaql.lang.rewrite.FunctionInline;
import com.ibm.jaql.lang.rewrite.GroupElimination;
import com.ibm.jaql.lang.rewrite.ImproveRecordConstruction;
import com.ibm.jaql.lang.rewrite.InjectAggregate;
import com.ibm.jaql.lang.rewrite.JoinToCogroup;
import com.ibm.jaql.lang.rewrite.LetInline;
import com.ibm.jaql.lang.rewrite.MapSplitPushdown;
import com.ibm.jaql.lang.rewrite.OuterJoinToInner;
import com.ibm.jaql.lang.rewrite.PathArrayToFor;
import com.ibm.jaql.lang.rewrite.PathIndexToFn;
import com.ibm.jaql.lang.rewrite.PerPartitionElimination;
import com.ibm.jaql.lang.rewrite.PragmaElimination;
import com.ibm.jaql.lang.rewrite.RetagExpand;
import com.ibm.jaql.lang.rewrite.RetagMerge;
import com.ibm.jaql.lang.rewrite.RewriteFirstPathStep;
import com.ibm.jaql.lang.rewrite.RewritePhase;
import com.ibm.jaql.lang.rewrite.SimplifyFirstNonNull;
import com.ibm.jaql.lang.rewrite.SimplifyRecord;
import com.ibm.jaql.lang.rewrite.SimplifyUnion;
import com.ibm.jaql.lang.rewrite.SplitMapToRetag;
import com.ibm.jaql.lang.rewrite.SplitSplitPushdown;
import com.ibm.jaql.lang.rewrite.SplitToWrite;
import com.ibm.jaql.lang.rewrite.TagFlattenPushdown;
import com.ibm.jaql.lang.rewrite.TagToTransform;
import com.ibm.jaql.lang.rewrite.TeeToDiamondSplit;
import com.ibm.jaql.lang.rewrite.ToArrayElimination;
import com.ibm.jaql.lang.rewrite.ToMapReduce;
import com.ibm.jaql.lang.rewrite.TransformMerge;
import com.ibm.jaql.lang.rewrite.TrivialCombineElimination;
import com.ibm.jaql.lang.rewrite.TrivialForElimination;
import com.ibm.jaql.lang.rewrite.TrivialTransformElimination;
import com.ibm.jaql.lang.rewrite.TypeCheckSimplification;
import com.ibm.jaql.lang.rewrite.UnionToComposite;
import com.ibm.jaql.lang.rewrite.UnnestFor;
import com.ibm.jaql.lang.rewrite.UnrollForLoop;
import com.ibm.jaql.lang.rewrite.UnrollTransformLoop;
import com.ibm.jaql.lang.rewrite.VarProjection;
import com.ibm.jaql.lang.rewrite.WriteAssignment;
import com.ibm.jaql.lang.walk.ExprFlow;
import com.ibm.jaql.lang.walk.ExprWalker;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/**
 * 
 */
public class RewriteEngine extends com.ibm.jaql.lang.rewrite.RewriteEngine {
	// protected int phaseId = 0;
	// protected RewritePhase[] phases = new RewritePhase[7];
	// protected boolean traceFire = false;
	// protected boolean explainFire = false; // traceFire must true for this to matter
	// protected long counter = 0;
	//
	// // These are work areas for use by rewrites.
	// public Env env;
	// public ExprWalker walker = new PostOrderExprWalker();
	// public ExprFlow flow = new ExprFlow();
	// public VarMap varMap = new VarMap();
	// public ArrayList<Expr> exprList = new ArrayList<Expr>();
	// public ArrayList<Expr> aggList = new ArrayList<Expr>();

	/**
   * 
   */
	public RewriteEngine() {
		int phaseId = 0;

		ExprWalker postOrderWalker = new PostOrderExprWalker();
		// ExprWalker rootWalker = new OneExprWalker();

		// ------------------------------------------------------------------------------------
		RewritePhase phase = phases[phaseId] = new RewritePhase(this, postOrderWalker, 10000);
		new LetInline(phase);
//		new DoMerge(phase);
//		new DoPullup(phase);
//		new FunctionInline(phase);
//		new TrivialForElimination(phase);
//		new TrivialTransformElimination(phase);
//		new AsArrayElimination(phase);
//		new DoConstPragma(phase);
//		new UnrollTransformLoop(phase);
//		new ImproveRecordConstruction(phase);
//		new SimplifyRecord(phase);
//		new CheapConstEval(phase);
//		new ConstIfElimination(phase);
//		new DaisyChainInline(phase);
//		new ForToLet(phase);
//		new DoInlinePragma(phase);
//		new ConstArrayAccess(phase); // FIXME: merge with RewriteFirstPathStep, remove fn
//		new ConstFieldAccess(phase); // FIXME: merge with RewriteFirstPathStep, remove fn
//		new ForInSimpleIf(phase);
//		new SimplifyFirstNonNull(phase);
//		new TrivialCombineElimination(phase);
//		new CombineInputSimplification(phase);
//		new ToArrayElimination(phase);
//		new EmptyOnNullElimination(phase);
//		new InjectAggregate(phase);
//		new SimplifyUnion(phase);
//		new VarProjection(phase);
//		new WriteAssignment(phase);
//		new TypeCheckSimplification(phase);
//		new UnrollForLoop(phase);
//		new FilterPredicateSimplification(phase);
//		new FilterPushDown(phase);
//		new FilterMerge(phase);
//		new OuterJoinToInner(phase);
//		// ---tee part 1 ---
//		new TeeToDiamondSplit(phase);
//		new MapSplitPushdown(phase);
//		new SplitMapToRetag(phase);
//		new RetagMerge(phase);
//		new SplitSplitPushdown(phase);
//		new FlattenTagSplit(phase);
		// new TeeToBlock(phase);
		// ---end:tee---

		// ------------------------------------------------------------------------------
		phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
//		new LetInline(phase);
//		new DoMerge(phase);
//		new DoPullup(phase);
//		new FunctionInline(phase);
//		new TrivialForElimination(phase);
//		new TrivialTransformElimination(phase);
//		new AsArrayElimination(phase);
//		new DoConstPragma(phase);
//		new UnrollTransformLoop(phase);
//		new ImproveRecordConstruction(phase);
//		new SimplifyRecord(phase);
//		new CheapConstEval(phase);
//		new ConstIfElimination(phase);
//		new FilterPredicateSimplification(phase);
//		new FilterPushDown(phase);
//		new FilterMerge(phase);
//		new OuterJoinToInner(phase);
//		new UnrollForLoop(phase);
//		new UnrollTransformLoop(phase);
//		new SimplifyUnion(phase);
//		// ----tee part 2 ------
//		new DiamondTagExpand(phase);
//		new RetagExpand(phase);
//		new SplitToWrite(phase);
//		new TagFlattenPushdown(phase);
//		// ----end;tee------

		// ------------------------------------------------------------------------------
		RewritePhase basicPhase = new RewritePhase(this, postOrderWalker, 10000);
		phase = phases[++phaseId] = basicPhase;
//		new LetInline(phase);
//		new DoMerge(phase);
//		new DoPullup(phase);
//		// new DechainFor(phase);
//		new FunctionInline(phase);
//		new DaisyChainInline(phase);
//		new TrivialForElimination(phase);
//		new TrivialTransformElimination(phase);
//		new TransformMerge(phase);
//		new ForToLet(phase);
//		new AsArrayElimination(phase);
//		// new GlobalInline(phase);
//		new DoInlinePragma(phase);
//		new ConstArrayAccess(phase); // FIXME: merge with RewriteFirstPathStep, remove fn
//		new ConstFieldAccess(phase); // FIXME: merge with RewriteFirstPathStep, remove fn
//		new ForInSimpleIf(phase);
//		new SimplifyFirstNonNull(phase);
//		new TrivialCombineElimination(phase);
//		new CombineInputSimplification(phase);
//		new DoConstPragma(phase);
//		new RewriteFirstPathStep(phase);
//		new PathArrayToFor(phase); // FIXME: merge with RewriteFirstPathStep
//		new PathIndexToFn(phase); // FIXME: merge with RewriteFirstPathStep, remove PathIndexToFn + fn, remove
//									// ConstFieldAccess, et al
//		new ToArrayElimination(phase);
//		new EmptyOnNullElimination(phase);
//		new InjectAggregate(phase);
//		new UnrollForLoop(phase);
//		new UnrollTransformLoop(phase);
//		new SimplifyUnion(phase);
//		new UnionToComposite(phase);
//		new VarProjection(phase);
//		new ImproveRecordConstruction(phase);
//		new SimplifyRecord(phase);
//		new UnnestFor(phase);
//		new WriteAssignment(phase);
//		new TypeCheckSimplification(phase);
//		// new ConstEval(phase); // TODO: do we need full ConstEval? Should it be in this or another phase? Can it be
//		// made quicker?
//		new CheapConstEval(phase);
//		new ConstIfElimination(phase);
//		new ConstJumpElimination(phase);
//		// new StrengthReduction(phase);
//		// new ConstArray(phase);
//		// new ConstRecord(phase);
//		// ----tee part 3 ------
//		new TagToTransform(phase); // TODO: need TagFlattenToTransform?
//		// ----end;tee------

		// ------------------------------------------------------------------------------
		phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
		// new GroupToMapReduce(phase);
//		new JoinToCogroup(phase);
		// new TempSharedVariable(phase);
//		new WriteAssignment(phase);
//		new UnionToComposite(phase);
		// new CogroupToMapReduce(phase);
		// new ForToMapReduce(phase);

		// // TODO: put ConstEval in basicPhase? it is somewhat expensive because it
		// // tested on every expr and the test can walk a lot of the tree...
		// phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 1000);
		// new ConstEval(phase); // TODO: run bottom-up/post-order
		// new LetInline(phase); // ConstEval opens more LetInline chances, which opens more ConstEval
		// //new ConstFunction(phase);

		// ------------------------------------------------------------------------------
		// phase = phases[++phaseId] = new RewritePhase(this, rootWalker, 1);
		phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 1000);
//		new ToMapReduce(phase);
//		new WriteAssignment(phase);
//		new LetInline(phase);
//		new DoMerge(phase);
//		new DoPullup(phase);
//		new UnionToComposite(phase);

		// ------------------------------------------------------------------------------
		phase = phases[++phaseId] = new RewritePhase(this, postOrderWalker, 10000);
//		new GroupElimination(phase);
//		new PerPartitionElimination(phase);
//		new PragmaElimination(phase);

		phases[++phaseId] = phases[1];
	}

	/**
	 * @param env
	 * @param stmt
	 * @throws Exception
	 */
	public Expr run(Expr stmt) throws Exception {
		// if (true) return query;

		// We don't rewrite def expressions until they are actually evaluated.
		// FIXME: rewrites of MaterializeExpr inlines functions; disable those inlines
		// if (query instanceof AssignExpr || query instanceof MaterializeExpr)
		// if (query instanceof AssignExpr )
		// {
		// return query;
		// }
		if (stmt.getEnvExpr() == null) {
			throw new IllegalArgumentException("expression tree does not have an EnvExpr");
		}
		this.env = stmt.getEnvExpr().getEnv();
		if (env == null) {
			throw new IllegalArgumentException("expression tree does not have an environment");
		}
		// We always run getSchema at the top to get variable schemas set. There should be a better way...
		Schema schema = stmt.getSchema();
		if (traceFire) {
			System.err.print("start ");
			System.err.println(schema);
		}
		counter = 0;
		for (RewritePhase phase : phases) {
			phase.run(stmt);
		}
		if (traceFire) {
			schema = stmt.getSchema();
			System.err.print("end ");
			System.err.println(schema);
		}
		return stmt;
	}

	/**
	 * @return a unique number for this run of the engine
	 */
	public long counter() {
		long n = counter;
		counter++;
		return n;
	}
}
