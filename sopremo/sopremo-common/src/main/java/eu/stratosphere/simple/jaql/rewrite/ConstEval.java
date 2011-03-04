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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FieldExpr;
import com.ibm.jaql.lang.expr.top.EnvExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * 
 */
public class ConstEval extends Rewrite
{
  /**
   * @param phase
   */
  public ConstEval(RewritePhase phase)
  {
    super(phase, Expr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    // We need to be careful computing small functions that produce large results.
    // Those functions mark themselves as not compile time computable.
    if (expr instanceof ConstExpr || expr instanceof BindingExpr || expr instanceof EnvExpr
        || expr instanceof FieldExpr || expr.isCompileTimeComputable().maybeNot())
    {
      return false;
    }

    JsonValue value = expr.getEnvExpr().getEnv().eval(expr);
    ConstExpr c = new ConstExpr(value);
    expr.replaceInParent(c);
    // context.reset(); // TODO: need to wrap up parse, eval, cleanup into one class and use everywhere
    return true;
  }
}
