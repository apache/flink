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

import com.ibm.jaql.lang.expr.agg.CombineExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * combine $a,$b in f(e1) using e2 ==> combine $a, $b in e1 using e2 where f in {
 * emptyOnNull, asArray, nullOnEmpty }
 */
public class CombineInputSimplification extends Rewrite
{
  /**
   * @param phase
   */
  public CombineInputSimplification(RewritePhase phase)
  {
    super(phase, CombineExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    // TODO: reenable with function api?
//    CombineExpr ce = (CombineExpr) expr;
//    BindingExpr bind = null; // ce.binding();
//    expr = bind.inExpr();
//    while (expr instanceof EmptyOnNullFn || expr instanceof NullOnEmptyFn
//        || expr instanceof AsArrayFn)
//    {
//      expr = expr.child(0);
//    }
//    if (expr != bind.inExpr())
//    {
//      bind.setChild(0, expr);
//      return true;
//    }
    return false;
  }
}
