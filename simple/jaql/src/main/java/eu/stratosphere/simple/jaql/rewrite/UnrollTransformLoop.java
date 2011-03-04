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

import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.pragma.UnrollLoopPragma;
import com.ibm.jaql.lang.rewrite.RewritePhase;
import com.ibm.jaql.lang.rewrite.UnrollLoop;

/**
 * [e1,e2,...,en] -> transform each i er(i) 
 * ==> 
 * [ (i1 = e1, er(i1)),
 *   (i2 = e2, er(i2)), 
 *   ...,
 *   (in = en, er(in)) ]
 */
public class UnrollTransformLoop extends UnrollLoop
{
  /**
   * @param phase
   */
  public UnrollTransformLoop(RewritePhase phase)
  {
    super(phase, TransformExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    TransformExpr transform = (TransformExpr) expr;
    Expr[] exprs = buildExprList(transform.binding(), transform.projection());
    if( exprs == null )
    {
      return false;
    }
    ArrayExpr ae = new ArrayExpr(exprs);
    if( transform.parent() instanceof UnrollLoopPragma )
    {
      expr = transform.parent();
    }
    expr.replaceInParent(ae);
    return true;
  }
}
