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
import com.ibm.jaql.lang.expr.io.AbstractWriteExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 *     e1 -> tagSplit( ..., gn ) -> m+ -> w? -> e2
 *        where m+ is a sequence of mappable expressions
 *              w? is an optional write expression
 * ==>
 *     e1 -> tagSplit(..., fn(x) gn(x) -> m+ -> w? ) -> e2
 */
public class MapSplitPushdown extends Rewrite
{
  public MapSplitPushdown(RewritePhase phase)
  {
    super(phase, TagSplitFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr split)
  {
    assert split instanceof TagSplitFn;

    // find: split() -> m* -> w?
    //    where m or w exists
    Expr pipe = findPipeline(split, null);
    if( pipe == null )
    {
      if( split.parent() instanceof AbstractWriteExpr )
      {
        pipe = split.parent();
      }
      else
      {
        return false;
      }
    }
    else if( pipe.parent() instanceof AbstractWriteExpr )
    {
      pipe = pipe.parent();
    }

    // gfn = fn($gv) gfn($gv) -> m+ -> w?
    int n = split.numChildren() - 1;
    Expr gfn = split.child(n);
    Var gv = engine.env.makeVar("$");
    Expr gfnCall = new FunctionCallExpr(gfn, new VarExpr(gv));
    split.replaceInParent(gfnCall);
    
    // e1 -> tagSplit(...) -> e2
    pipe.replaceInParent(split);
    
    // e1 -> tagSplit(..., gfn) -> e2
    gfn = new DefineJaqlFunctionExpr(new Var[]{gv}, pipe);
    split.setChild(n, gfn);

    return true;
  }

}
