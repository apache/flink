/*
 * Copyright (C) IBM Corp. 2009.
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

import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * e1 -> transform $1 e2 -> op each $2 e3
 *    where op is iterating over the transform
 *          for now, op in transform, expand, group, sort
 *      and e3 uses transform result at most once
 * 
 *   e1 -> op each $1 e3[$2<=e2]
 * 
 */
public class TransformMerge extends Rewrite
{
  /**
   * @param phase
   */
  public TransformMerge(RewritePhase phase)
  {
    super(phase, TransformExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    TransformExpr te = (TransformExpr) expr;
    expr = te.parent();
    if( !(expr instanceof BindingExpr) )
    {
      return false;
    }
    
    BindingExpr b2 = (BindingExpr)expr;
    expr = b2.parent();
    // Note: cannot inline: e1 -> transform e2 -> sort by (e3($)) 
    //                  as: e1 -> sort by (e3(e2($))
    //    because e2 is also implicit output of sort!
    if( !( expr instanceof TransformExpr ||
           // expr instanceof GroupByExpr || // doesn't work on group by either because it travels through the "as" variable.
           expr instanceof ForExpr ) ) // TODO: add more: create API for b2.parent().iteratesOver(b2) ?
    {
      return false;
    }
    
    ArrayList<Expr> uses = engine.exprList;
    uses.clear();
    expr.getVarUses(b2.var, uses);
    if( uses.size() > 1 )
    {
      return false;
    }
    
    BindingExpr b1 = te.binding();
    replaceVarUses(b2.var, expr, te.projection());
    b2.var = b1.var;
    te.replaceInParent(b1.inExpr());
    return true;
  }
}
