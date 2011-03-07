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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.FieldExpr;
import com.ibm.jaql.lang.expr.top.TopExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Perform compile-time constant evaluation by only looking at the parent of a ConstExpr.
 * This misses any compile-time evaluable expressions that do not have any constant arguments
 * (e.g., say we had a function pi() that returned a constant). However, it is significantly
 * faster than the ConstEval that tests on every Expr.
 */
public class CheapConstEval extends Rewrite
{
  public CheapConstEval(RewritePhase phase)
  {
    super(phase, ConstExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    boolean fired = false;
    while( true )
    {
      expr = expr.parent(); // expr is now the parent of a ConstExpr
      if( expr instanceof TopExpr )
      {
        return fired;
      }
      // TODO: we should have something like: while( expr instanceof StructuralExpr ) expr = expr.parent() (ie, the real expr is above)
      if( expr instanceof BindingExpr ||
          expr instanceof FieldExpr )
      {
        expr = expr.parent();
      }

      // A fast check for an compile-time computable expression that is 
      // at most two levels from the leaf of the tree.
      // The full check does a deep walk of the whole tree that we are trying to avoid.
      if( expr.getProperty(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, false).maybeNot() )
      {
        return fired;
      }
      for( Expr e: expr.children() )
      {
        for( Expr e2: e.children() )
        {
          if( e2.numChildren() > 0 )
          {
            return fired;
          }
        }
      }
      // Do the full check, now that we know we don't have a big tree.
      // All of this can go away when better property caching in the tree.
      if( expr.isCompileTimeComputable().maybeNot() )
      {
        return fired;
      }

      JsonValue value = expr.compileTimeEval();
      ConstExpr c = new ConstExpr(value);
      expr.replaceInParent(c);
      expr = c;
      fired = true;
    }
  }
}
