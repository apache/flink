/*
 * Copyright (C) IBM Corp. 2010.
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

import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JumpFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * If the branch of a jump is constant, remove the jump.
 *   jump(i, e1, ..., en) ==> ei
 */
public class ConstJumpElimination extends Rewrite
{
  public ConstJumpElimination(RewritePhase phase)
  {
    super(phase, JumpFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    JumpFn jump = (JumpFn)expr;
    Expr index = jump.child(0);
    if( !(index instanceof ConstExpr) )
    {
      return false;
    }

    ConstExpr ce = (ConstExpr)index;
    JsonNumber jn = (JsonNumber)ce.value;
    int i = jn.intValueExact();
    Expr ei = jump.child(i+1);
    jump.replaceInParent(ei);
    return true;
  }
}
