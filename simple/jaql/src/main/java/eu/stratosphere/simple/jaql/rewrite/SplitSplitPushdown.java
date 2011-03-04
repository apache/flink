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

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TagSplitFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 *     e1 -> tagSplit2( ..., g ) -> tagSplit1(...) -> e2
 * ==>
 *     e1 -> tagSplit2(..., fn(x) g(x) -> tagSplit1(...) ) -> e2
 */
public class SplitSplitPushdown extends Rewrite
{
  public SplitSplitPushdown(RewritePhase phase)
  {
    super(phase, TagSplitFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr split1)
  {
    assert split1 instanceof TagSplitFn;

    Expr split2 = split1.child(0);
    if( !(split2 instanceof TagSplitFn ) )
    {
      return false;
    }
    
    // gfn(x) -> tagSplit1(...)
    int n = split2.numChildren() - 1;
    Expr gfn = split2.child(n);
    Var gv = engine.env.makeVar("$");
    Expr gfnCall = new FunctionCallExpr(gfn, new VarExpr(gv));
    split1.setChild(0, gfnCall);
    
    // e1 -> tagSplit2(...) -> e2
    split1.replaceInParent(split2);
    
    // gfn = fn(x) gfn(x) -> tagSplit1(...)
    gfn = new DefineJaqlFunctionExpr(new Var[]{gv}, split1);

    // e1 -> tagSplit(..., gfn) -> e2
    split2.setChild(n, gfn);

    return true;
  }

}
