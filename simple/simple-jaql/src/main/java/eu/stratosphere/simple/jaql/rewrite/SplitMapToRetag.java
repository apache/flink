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

import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.RetagFn;
import com.ibm.jaql.lang.expr.core.TagSplitFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 *     e1 -> tagSplit( -> m1* -> e1, ..., -> mn* -> en )
 *        where mi* is a pipe (a sequence of zero or more mappable expressions)
 *          and at least one mi exists
 *          and 
 * ==>
 *     e1 -> retag( -> m1*, ..., -> mn* ) -> tagSplit( -> e1, ..., -> en )
 * 
 */
public class SplitMapToRetag extends Rewrite
{
  public SplitMapToRetag(RewritePhase phase)
  {
    super(phase, TagSplitFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr split)
  {
    assert split instanceof TagSplitFn;
    int n = split.numChildren() - 1;
    HashSet<Var> safeVars = null;

    // Find fi that has a mappable part in the beginning
    ArrayList<Expr> uses = engine.exprList;
    Expr[] retagArgs = null;
    for( int i = 1 ; i <= n ; i++ )
    {
      Expr fnExpr = split.child(i);
      if( fnExpr instanceof DefineJaqlFunctionExpr )
      {
        DefineJaqlFunctionExpr fn = (DefineJaqlFunctionExpr)fnExpr;
        if( fn.numParams() == 1 ) // support default args?
        {
          Expr body = fn.body();
          Var fvar = fn.varOf(0);
          uses.clear();
          body.getVarUses(fvar, uses);
          if( uses.size() == 1 )
          {
            VarExpr fvarUse = (VarExpr)uses.get(0);
            // This test is redundant with findPipeline, but avoids more 
            // expensive work when we cannot rewrite
            if( fvarUse.parent() instanceof BindingExpr ||
                fvarUse.parent().isMappable(fvarUse.getChildSlot()) )
            {
              if( safeVars == null )
              {
                safeVars = split.getCapturedVars();
              }
              safeVars.add(fvar);
              Expr pipe = findPipeline(fvarUse, safeVars);
              safeVars.remove(fvar);

              if( pipe != null )
              {
                // found: f = fn(x) x -> pipe -> rest
                // we are rewriting!
                if( retagArgs == null )
                {
                  retagArgs = new Expr[n+1];
                }

                // make: f = fn(x) x -> rest
                pipe.replaceInParent(new VarExpr(fvar));

                // make: g = fn(y) y -> pipe
                Var gvar = engine.env.makeVar("$");
                fvarUse.replaceInParent(new VarExpr(gvar));
                Expr gfn = new DefineJaqlFunctionExpr(new Var[]{gvar}, pipe);
                retagArgs[i] = gfn;
              }
            }
          }
        }
      }
    }
    
    if( retagArgs == null )
    {
      return false;
    }

    // For any leg that wasn't mapping, make an identity function
    for(int i = 1 ; i <= n ; i++)
    {
      if( retagArgs[i] == null )
      {
        // make: g = fn(y) y
        Var gvar = engine.env.makeVar("$");
        retagArgs[i] = new DefineJaqlFunctionExpr(new Var[]{gvar}, new VarExpr(gvar));
      }
    }

    // e1 -> tagSplit(...) ==> e1 -> retag(...) -> tagSplit(...)
    retagArgs[0] = split.child(0);
    split.setChild(0, new RetagFn(retagArgs));
    return true;
  }

}
