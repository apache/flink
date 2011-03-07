package eu.stratosphere.simple.jaql.rewrite;

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.rewrite.RewritePhase;
import com.ibm.jaql.lang.walk.ExprWalker;

public abstract class Rewrite extends com.ibm.jaql.lang.rewrite.Rewrite {

	public Rewrite(RewritePhase phase, Class<? extends Expr> fireOn) {
		super(phase, fireOn);
	}

	public Rewrite(RewritePhase phase, Class<? extends Expr>[] fireOn) {
		super(phase, fireOn);
	}

	/**
	 * @param expr
	 * @param var
	 * @return
	 */
	public int countVarUse(Expr expr, Var var) {
		int n = 0;
		ExprWalker walker = engine.walker;
		walker.reset(expr);
		while ((expr = walker.next()) != null) {
			if (expr instanceof VarExpr) {
				VarExpr ve = (VarExpr) expr;
				if (ve.var() == var) {
					n++;
				}
			}
		}
		return n;
	}

	/**
	 * @param expr
	 * @param var
	 * @return
	 */
	public VarExpr findFirstVarUse(Expr expr, Var var) {
		ExprWalker walker = engine.walker;
		walker.reset(expr);

		while ((expr = walker.next()) != null) {
			if (expr instanceof VarExpr) {
				VarExpr ve = (VarExpr) expr;
				if (ve.var() == var) {
					return (VarExpr) expr;
				}
			}
		}
		return null;
	}

	/**
	 * Find PathExprs and VarExprs(that are not children of PathExpr) that contain the given var
	 */
	public ArrayList<Expr> findMaximalVarOrPathExpr(Expr expr, Var var) {
		ExprWalker walker = engine.walker;
		walker.reset(expr);
		ArrayList<Expr> list = new ArrayList<Expr>();

		while ((expr = walker.next()) != null) {
			if (expr instanceof VarExpr) {
				VarExpr ve = (VarExpr) expr;
				if (ve.var() == var) {
					if ((expr.parent() instanceof PathExpr) && (expr.getChildSlot() == 0))
						list.add((PathExpr) expr.parent());
					else
						list.add((VarExpr) expr);
				}
			}
		}
		return list;
	}

}
