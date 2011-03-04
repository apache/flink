/*
 * Copyright (C) IBM Corp. 2010.
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

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TeeExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

// FIXME: currently unused in favor of special handling of tee.  This might come back to life.

/**
 * Expand tee into a block.  // TODO: make tee into a macro?
 * 
 * e1 -> tee( e2, ... ) -> e3
 * =>
 * ( t = e1, e2(t), ..., t ) -> e3 
 *     // FIXME: should it be: ( t = asArray(e1), e2(t), ..., t ) -> e3 
 * 
 */
public class TeeToBlock extends Rewrite
{
  /**
   * @param phase
   */
  public TeeToBlock(RewritePhase phase)
  {
    super(phase, TeeExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    TeeExpr tee = (TeeExpr) expr;
    int n = tee.numChildren();

    Expr[] exprs = new Expr[n+1];
    Var var = engine.env.makeVar("t");

    // FIXME: we're assuming that tee does not force an array.  It does right now, but why should it?
    // FIXME: change tee to work with any input 
//    Expr e = tee.child(0);
//    if( e.getSchema().is(JsonType.ARRAY).maybeNot() )
//    {
//      e = new AsArrayFn(e);
//    }
    
    exprs[0] = new BindingExpr(BindingExpr.Type.EQ, var, null, tee.child(0));
    for(int i = 1 ; i < n ; i++)
    {
      exprs[i] = new FunctionCallExpr(tee.child(i), new VarExpr(var));
    }
    exprs[n] = new VarExpr(var);
    
    tee.replaceInParent(new DoExpr(exprs));
    return true;
  }
}
