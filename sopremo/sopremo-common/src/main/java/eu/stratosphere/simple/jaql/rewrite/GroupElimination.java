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
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * e1 -> group by const as $ expand e2($)
 * ==>
 * ($ = e1, expand e2)
 * 
 */
public class GroupElimination extends Rewrite
{
  /**
   * @param phase
   */
  public GroupElimination(RewritePhase phase)
  {
    super(phase, GroupByExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    GroupByExpr ge = (GroupByExpr) expr;
    if( ge.numInputs() != 1 || ge.byBinding().isCompileTimeComputable().maybeNot() )
    {
      return false;
    }
    BindingExpr b = ge.inBinding();
    b.type = BindingExpr.Type.EQ;
    b.var = ge.getAsVar(0);
    Expr e = new DoExpr(b, ge.collectExpr());
    ge.replaceInParent(e);
    return true;
  }
}
