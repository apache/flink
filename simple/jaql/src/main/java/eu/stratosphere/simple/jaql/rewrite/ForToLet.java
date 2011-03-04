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

import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * for( $i in [e1] ) e2 ==> ( $i = e1, asArray(e2) )
 */
public class ForToLet extends Rewrite
{
  /**
   * @param phase
   */
  public ForToLet(RewritePhase phase)
  {
    super(phase, ForExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    ForExpr fe = (ForExpr) expr;
    BindingExpr bind = fe.binding();
    Expr inExpr = bind.inExpr();

    if (!(inExpr instanceof ArrayExpr) || inExpr.numChildren() != 1)
    {
      return false;
    }

    Expr elem = inExpr.child(0);
    Expr ret = fe.collectExpr();
    if (ret.getSchema().is(ARRAY,NULL).maybeNot())
    {
      ret = new AsArrayFn(ret);
    }
    else if (ret.getSchema().is(NULL).maybe())
    {
      ret = new EmptyOnNullFn(ret);
    }
    bind.type = BindingExpr.Type.EQ;
    bind.setChild(0, elem);
    Expr doExpr = new DoExpr(bind, ret);
    fe.replaceInParent(doExpr);
    return true;
  }
}
