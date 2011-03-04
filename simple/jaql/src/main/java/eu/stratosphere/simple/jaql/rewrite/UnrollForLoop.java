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

import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.pragma.UnrollLoopPragma;
import com.ibm.jaql.lang.rewrite.RewritePhase;
import com.ibm.jaql.lang.rewrite.UnrollLoop;

/**
 * [e1,e2,...,en] -> expand each i er(i) 
 * ==> 
 * merge(
 *   (i1 = e1, er(i1)),
 *   (i2 = e2, er(i2)), 
 *   ...,
 *   (in = en, er(in)) )
 */
public class UnrollForLoop extends UnrollLoop
{
  /**
   * @param phase
   */
  public UnrollForLoop(RewritePhase phase)
  {
    // super(phase, UnrollLoopPragma.class);
    super(phase, ForExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
//    UnrollLoopPragma pragma = (UnrollLoopPragma)expr;
//    if( !( pragma.child(0) instanceof ForExpr ) )
//    {
//      return false;
//    }
//    ForExpr fe = (ForExpr) pragma.child(0);
    ForExpr forExpr = (ForExpr) expr;
    Expr[] exprs = buildExprList(forExpr.binding(), forExpr.collectExpr());
    if( exprs == null )
    {
      return false;
    }
    
    UnionFn merge = new UnionFn(exprs);
//    pragma.replaceInParent(merge);
    if( forExpr.parent() instanceof UnrollLoopPragma )
    {
      expr = forExpr.parent();
    }
    expr.replaceInParent(merge);
    return true;
  }
}
