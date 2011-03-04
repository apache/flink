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
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FilterExpr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.schema.CheckFn;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * fn( (e1,...,e2), ..., e3) ==> ( e1, ..., fn(e2,...,e3) ) 
 *
 * ( (e1,...,e2) )(e3,...) ==> 
 */
public class DoPullup extends Rewrite
{
  /**
   * @param phase
   */
  public DoPullup(RewritePhase phase)
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
    
    Expr originalDoParent = doExpr.parent();
    int originalDoSlot = doExpr.getChildSlot();
    Expr fn = originalDoParent;

    if( fn instanceof BindingExpr )
    {
      Expr bind = fn;
      fn = bind.parent();
      if( ( fn instanceof TransformExpr ||
            fn instanceof FilterExpr ||
            fn instanceof ForExpr ||
            fn instanceof GroupByExpr )
          && bind.getChildSlot() == 0 )
      {
        // we can safely rewrite these cases
      }
      else
      {
        return false;
      }
    }
    else if( fn.evaluatesChildOnce(originalDoSlot).maybeNot() || // Do not change the number of times the DoExpr is evaluated
             fn instanceof ExplainExpr || 
             fn instanceof QueryExpr ||
             fn instanceof DoExpr ||
             fn instanceof IfExpr ||
             fn instanceof CheckFn )
    {
      // FIXME: if( parent is control-flow ) return false
      //    if( e.getProperty(ExprProperty.IS_CONTROL_FLOW, false).maybe() )
      return false;
    }
    
    // fn( ..., e1, do(e2,...,e3), e4, ...)
    // ==>
    // do(e2,...,fn( ..., e1,e3,e4, ...))
    
    Expr ret = doExpr.returnExpr();
    fn.replaceInParent(doExpr);
    ret.replaceInParent(fn);
    originalDoParent.setChild(originalDoSlot, ret);

    return true;
  }
}
