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
import com.ibm.jaql.lang.expr.pragma.Pragma;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Removed unapplied pragmas from the tree.
 */
public class PragmaElimination extends Rewrite
{
  /**
   * @param phase
   */
  public PragmaElimination(RewritePhase phase)
  {
    super(phase, Pragma.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    Pragma pragma = (Pragma)expr;
    if( pragma.numChildren() != 1 )
    {
      System.err.println("Pragma was not applied, but cannot be removed...");  // Could this ever happen?!
      return false;
    }
    System.err.println("Pragma was ignored: "+pragma);
    pragma.replaceInParent(pragma.child(0));
    return true;
  }
}
