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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TagFlattenFn;
import com.ibm.jaql.lang.expr.core.TagSplitFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Flatten nested tagSplits
 *     e1 -> tagSplit(f1, ..., fi = -> tagSplit(g1, ..., gM), ..., fN)
 * ==>
 *     e1 -> flattenTag(i,M) -> tagSplit(f1, ..., g1, ..., gM, ..., fN)
 */
public class FlattenTagSplit extends Rewrite
{
  public FlattenTagSplit(RewritePhase phase)
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
    int n = split.numChildren() - 1;
    
    // Find fi that is a tagSplit
    int i;
    for( i = 1 ; i <= n ; i++ )
    {
      Expr fnExpr = split.child(i);
      if( fnExpr instanceof DefineJaqlFunctionExpr )
      {
        DefineJaqlFunctionExpr fi = (DefineJaqlFunctionExpr)fnExpr;
        if( fi.numParams() == 1 ) // support default args?
        {
          Expr split2 = fi.body();
          if( split2 instanceof TagSplitFn )
          {
            if( split2.child(0) instanceof VarExpr )
            {
              VarExpr ve = (VarExpr)split2.child(0);
              if( ve.var() == fi.varOf(0) )
              {
                // We found fn(x) x -> tagSplit(...)
                if( this.countVarUse(split2, ve.var()) == 1 )
                {
                  // and ... doesn't use x again, so we can rewrite!
                  // e1 -> tagSplit1(f1, ..., fi = -> tagSplit2(g1, ..., gM), ..., fN)
                  // ==>
                  // e1 -> flattenTag(i-1,M) -> tagSplit(f1, ..., g1, ..., gM, ..., fN)
                  Expr[] gs = split2.children();
                  int m = gs.length-1;
                  fi.replaceInParent(gs, 1, m);
                  Expr flatten = new TagFlattenFn(
                      split.child(0), 
                      new ConstExpr(new JsonLong(i-1)),
                      new ConstExpr(new JsonLong(m)));
                  split.setChild(0, flatten);
                  return true;
                }
              }
            }
          }
        }
      }
    }
    
    // no nested function found
    return false;
  }

}
