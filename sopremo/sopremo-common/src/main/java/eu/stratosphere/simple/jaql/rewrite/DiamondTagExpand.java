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
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DiamondTagFn;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.TagFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Expand diamondTag into its definition:
 *     e1 -> diamondTag( g1, ..., gN )
 * ==>
 *     e1 -> expand each i union( g1([i]) -> transform [0,$], ..., gn([i]) -> transform [n,$] )
 */
public class DiamondTagExpand extends Rewrite
{
  public DiamondTagExpand(RewritePhase phase)
  {
    super(phase, DiamondTagFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr diamond)
  {
    assert diamond instanceof DiamondTagFn;
    int n = diamond.numChildren() - 1;
    Expr input = diamond.child(0);
    Var forVar = engine.env.makeVar("i");
    
    // make merge(g1(i) -> tag(1), ..., gn(i) -> tag(N))
    Expr[] args = new Expr[n];
    for(int i = 0 ; i < n ; i++ )
    {
//      Var trVar = engine.env.makeVar("$");
      Expr g = diamond.child(i+1);
      Expr call = new FunctionCallExpr(g, new ArrayExpr(new VarExpr(forVar)));
      args[i] = new TagFn(call, new ConstExpr(i));
//      Expr pair = new ArrayExpr(new ConstExpr(i), new VarExpr(trVar));
//      args[i] = new TransformExpr(trVar, call, pair);;
    }
    Expr union = new UnionFn(args);
    
    // make: e1 -> expand each i (union)
    Expr expr = new ForExpr(forVar, input, union);

    // replace diamond
    diamond.replaceInParent(expr);
    return true;
  }

}
