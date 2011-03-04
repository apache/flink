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

import com.ibm.jaql.lang.expr.core.DaisyChainFn;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Expand a daisy chain of function calls.
 * This works when the function list is compile-time computable or
 * when the function list expression is a literal array:
 * 
 *   daisyChain(input, [f1,f2,...,fn]) 
 *   ==>
 *   input -> f1() -> f2() -> ... -> fn()
 * 
 */
public class DaisyChainInline extends Rewrite
{
  /**
   * @param phase
   */
  public DaisyChainInline(RewritePhase phase)
  {
    super(phase, DaisyChainFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    DaisyChainFn daisy = (DaisyChainFn)expr;
    return daisy.inlineIfPossible();
  }
}
