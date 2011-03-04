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

import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

import static com.ibm.jaql.json.type.JsonType.*;

// TODO: this can be generalized (with blow-up of e2):
// for $i in 
//    if <p1>
//    then <e1>
//    else <e2>
// collect <e3>
//==>
// if <p1> then
//   for $i in <e1> 
//   collect <e2>
// else
//   for $i in <e2> 
//   collect <e3>

/**
 * for $i in if <p1> then <e1> else null | [] collect <e2> ==> if <p1> then for
 * $i in <e1> collect <e2> else []
 * 
 */
public class ForInSimpleIf extends Rewrite
{
  /**
   * @param phase
   */
  public ForInSimpleIf(RewritePhase phase)
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
    if (!(bind.inExpr() instanceof IfExpr))
    {
      return false;
    }

    IfExpr ifExpr = (IfExpr) bind.inExpr();
    Expr falseExpr = ifExpr.falseExpr();

    if (falseExpr.getSchema().isEmpty(ARRAY,NULL).maybeNot())
    {
      return false;
    }

    if (falseExpr.getSchema().is(NULL).maybe())
    {
      falseExpr.replaceInParent(new ArrayExpr());
    }

    Expr trueExpr = ifExpr.trueExpr();
    fe.replaceInParent(ifExpr);
    bind.setChild(0, trueExpr); // put <e1> as for-in
    ifExpr.setChild(1, fe); // put for as true branch of if
    return true;
  }
}
