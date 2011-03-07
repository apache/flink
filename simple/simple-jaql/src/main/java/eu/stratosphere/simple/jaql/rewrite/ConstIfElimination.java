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

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * If an if-test is constant, replace it with the expected branch:
 *   if( true  ) e1 else e2 ==> e1
 *   if( false ) e1 else e2 ==> e2
 */
public class ConstIfElimination extends Rewrite
{
  public ConstIfElimination(RewritePhase phase)
  {
    super(phase, IfExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    IfExpr ifExpr = (IfExpr)expr;
    Expr test = ifExpr.testExpr();
    
    if( !(test instanceof ConstExpr) ) // TODO: use this? expr.isCompileTimeComputable().always()
    {
      return false;
    }
    
    ConstExpr ce = (ConstExpr)test;
    JsonBool jb = (JsonBool)ce.value;
    if( jb == null || jb.get() == false )
    {
      ifExpr.replaceInParent(ifExpr.falseExpr());
    }
    else
    {
      ifExpr.replaceInParent(ifExpr.trueExpr());
    }
    return true;
  }
}
