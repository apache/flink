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

import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.ProxyExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.rewrite.RewritePhase;

// TODO: This rewrite possibly go away with the change in FOR definition to preserve input.

/**
 *     for($i in e1) for($j in e2($i)) e3($j) 
 * === e1 -> expand each $i (e2($i) -> expand each $j e3($j))
 * === e1 -> expand each $i e2($i) -> expand each $j e3($j)
 * 
 *     e1 -> expand each $i (e2($i) -> filter each $j e3($j))
 * === e1 -> expand each $i e2($i) -> filter each $j e3($j)
 *
 *     e1 -> expand each $i (e2($i) -> transform each $j e3($j))
 * === e1 -> expand each $i e2($i) -> transform each $j e3($j)
*/
public class UnnestFor extends Rewrite
{
  /**
   * @param phase
   */
  public UnnestFor(RewritePhase phase)
  {
    super(phase, ForExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    //     For                 Op
    //    /   \               /  \
    //   b1    Op      ==>   b2  e3
    //    |    / \            |
    //   e1   b2  e3         For
    //         |             /  \
    //        e2            b1  e2
    //                       |
    //                      e1
    
    ForExpr fe = (ForExpr)expr;
    Expr op = fe.collectExpr();
    
    // TODO: should we canonicalize Filter/Transform into For for rewrites, at least until late?
    // TODO: alternatively, make LoopExpr parent expr to catch all?
    if( op instanceof ForExpr ||
        op instanceof FilterExpr ||
        op instanceof TransformExpr )
    {
      Expr e3 = op.child(1);
      int n = countVarUse(e3, fe.var());
      if( n == 0 )
      {
        Expr proxy = new ProxyExpr();
        fe.replaceInParent(proxy);
        BindingExpr b2 = (BindingExpr)op.child(0);
        Expr e2 = b2.inExpr();
        fe.setChild(1, e2); // Place e2 below For
        b2.setChild(0, fe); // place For below Op binding
        proxy.replaceInParent(op);
        return true;
      }
    }
    
    return false;
  }
}
