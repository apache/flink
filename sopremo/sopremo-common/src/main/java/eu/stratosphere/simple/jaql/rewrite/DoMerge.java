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
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * ( e1 ) ==> e1, if e1 is not a BindingExpr
 *
 * ( e1, ..., v = (e2, ..., e3), e4, ... ) ==> ( e1, ..., e2, ..., v = e3, e4, ... )
 *  
 * ( e1, ..., (e2, ..., e3), e4, ... ) ==> ( e1, ..., e2, ..., e3, e4, ... ) 
 *
 */
public class DoMerge extends Rewrite // TODO: rename to Var inline
{
  /**
   * @param phase
   */
  public DoMerge(RewritePhase phase)
  {
    super(phase, DoExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    DoExpr doExpr = (DoExpr) expr;
    int n = doExpr.numChildren();

    assert n > 0; // There shouldn't be any empty do exprs

    if( n == 1 )
    {
      Expr e = doExpr.child(0);
      if( e instanceof BindingExpr )
      {
        // ( v = e ) ==> null, if e1 has no effects
        if( e.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never() &&
            e.getProperty(ExprProperty.IS_NONDETERMINISTIC, true).never() )
        {
          doExpr.replaceInParent(new ConstExpr(null));
          return true;
        }
        return false;
      }
      else
      {
        // ( e ) ==> e
        doExpr.replaceInParent(e);
        return true;
      }
    }
    
    Expr p = doExpr.parent();
    if( p instanceof DoExpr )
    {
      // ( e1, ..., (e2, ..., e3), e4, ... ) ==> ( e1, ..., e2, ..., e3, e4, ... )
      doExpr.replaceInParent(doExpr.children(), 0, n);
      return true;
    }
    
    if( p instanceof BindingExpr &&
        p.parent() instanceof DoExpr )
    {
      // ( e1, ..., v = (e2, ..., e3), e4, ... ) ==> ( e1, ..., e2, ..., v = e3, e4, ... )
      BindingExpr b = (BindingExpr)p;
      Expr e3 = doExpr.child(n-1);
      new BindingExpr(BindingExpr.Type.EQ, b.var, null, e3.injectAbove()); // (e2, ..., v = e3)
      b.replaceInParent(doExpr.children(), 0, n);
      return true;
    }

    return false;
  }
}
