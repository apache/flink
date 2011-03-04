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

import java.util.HashSet;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.FieldExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.io.AbstractReadExpr;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * for(i in e1) e2(i, e3())
 * 
 * ==>
 * 
 * ( temp = e3(),
 *   for(i in e1) e2(i, temp) )
 *
 * where e3 is not:
 *    ConstExpr
 *    VarExpr
 *    ReadExpr
 *    LocalReadExpr
 *    side-effecting or non-deterministic
 *
 * // TODO: This should be run on all flavors of FOR... (for/expand, transform, filter, etc)
 * 
 * // TODO: This could also be run on functions
 * x = fn(i) e1(i,e2())
 *  
 * ==>
 * 
 * x = (temp = e2, fn(i) e1(i,temp))
 * 
 */
public class StrengthReduction extends Rewrite // TODO: rename to Var inline
{
  protected Expr scope;
  
  /**
   * @param phase
   */
  public StrengthReduction(RewritePhase phase)
  {
    super(phase, Expr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    if( ! expr.isMappable(0) )
    {
      return false;
    }
    scope = expr;
    int n = expr.numChildren();
    for(int i = 1 ; i < n ; i++)
    {
      Expr c = expr.child(i);
      Expr indep = findIndependentExpr(c);
      if( indep instanceof AbstractReadExpr )
      {
        indep = indep.child(0);
      }
      if( ! isPromotable(indep) )
      {
        continue;
      }
      Var v = engine.env.makeVar("$temp", indep.getSchema()); // TODO: stash inline names to reuse here when possible
      indep.replaceInParent(new VarExpr(v));
      BindingExpr bind = new BindingExpr(BindingExpr.Type.EQ, v, null, indep);
      new DoExpr(bind, expr.injectAbove());
      return true;
    }
    
    return false;
  }
  
  protected Expr findIndependentExpr(Expr expr)
  {
    boolean allIndep = true;
    Expr best = null;
    for(Expr e: expr.children())
    {
      Expr c = findIndependentExpr(e);
      allIndep = (allIndep && c == e);
      if( best == null && isPromotable(c) )
      {
        best = c;
      }
    }

    // FIXME: if reading what is written, done
    if( !allIndep ||
        expr.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).maybe() ||
        expr.getProperty(ExprProperty.IS_NONDETERMINISTIC, true).maybe() )
    {
      return best;
    }

    return expr;
  }
  
  /**
   * expr is assumed to be an indepenedent expression that could be promoted.
   * This test is if expr is the root, do we want to promote it.
   * 
   * @param expr
   * @return
   */
  protected boolean isPromotable(Expr expr)
  {
    if( expr == null ||
        expr instanceof VarExpr ||
        expr instanceof ConstExpr ||
        expr instanceof BindingExpr ||
        expr instanceof FieldExpr ||
        expr instanceof PathStep ||
        expr instanceof AbstractReadExpr )
    {
      return false;
    }
    
    HashSet<Var> captures = expr.getCapturedVars();
    Expr safe = scope.parent();
    for( Var v: captures )
    {
      if( safe.findVarDef(v) == null )
      {
        return false;
      }
    }
    
    return true;
  }
}
