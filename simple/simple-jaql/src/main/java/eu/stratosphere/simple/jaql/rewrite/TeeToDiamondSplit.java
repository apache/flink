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
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.DiamondTagFn;
import com.ibm.jaql.lang.expr.core.TagSplitFn;
import com.ibm.jaql.lang.expr.core.TeeExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 *     e1 -> tee( f1,...,fn ) -> e2
 * ==>
 *     e1 -> tagDiamond( fn($) $, ..., fn($) $, fn($) $ ) -> tagSplit( f1, ..., fn, f($) $ ) 
 * 
 */
public class TeeToDiamondSplit extends Rewrite
{
  public TeeToDiamondSplit(RewritePhase phase)
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
    TeeExpr tee = (TeeExpr)expr;
    int n = tee.numChildren();
    Expr[] diamondLegs = new Expr[n + 1];
    diamondLegs[0] = tee.child(0);
    for(int i = 1 ; i <= n ; i++)
    {
      Var var = engine.env.makeVar("$");
      Expr body = new VarExpr(var);
      DefineJaqlFunctionExpr fn = new DefineJaqlFunctionExpr(new Var[]{var}, body);
      diamondLegs[i] = fn;
    }
    Expr diamond = new DiamondTagFn(diamondLegs);
    
    Expr[] splitLegs = new Expr[n + 1];
    splitLegs[0] = diamond;
    for(int i = 1 ; i < n ; i++)
    {
      splitLegs[i] = tee.child(i);
    }
    Var var = engine.env.makeVar("$");
    Expr body = new VarExpr(var);
    DefineJaqlFunctionExpr fn = new DefineJaqlFunctionExpr(new Var[]{var}, body);
    splitLegs[n] = fn;
    Expr split = new TagSplitFn(splitLegs);
    
    tee.replaceInParent(split);
    return true;
  }
}
