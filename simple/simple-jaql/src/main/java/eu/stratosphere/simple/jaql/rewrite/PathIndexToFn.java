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

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathIndex;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * e1 [e2]      ==> element(e1,e2)
 * e1 [e2] step ==> element(e1,e2) -> transform 
 */
public class PathIndexToFn extends Rewrite
{
  /**
   * @param phase
   */
  public PathIndexToFn(RewritePhase phase)
  {
    super(phase, PathIndex.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    if( !( expr.parent() instanceof PathExpr ) )
    {
      return false;
    }

    PathIndex pi = (PathIndex)expr;
    PathExpr pe = (PathExpr)pi.parent();
    PathStep nextStep = pi.nextStep();
    
    Expr e = new IndexExpr(pe.input(), pi.indexExpr());
    
    if( !(nextStep instanceof PathReturn) )
    {
      e = new PathExpr(e, nextStep);
    }
    pe.replaceInParent(e);
    return true;
  }
}
