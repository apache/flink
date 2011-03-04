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
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.core.JumpFn;
import com.ibm.jaql.lang.expr.core.RetagFn;
import com.ibm.jaql.lang.expr.core.TagFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Expand retag into its definition:
 * 
 *     e1 -> retag( g1, ..., gN )
 * ==>
 *     e1 -> expand each p ( jump(p[0], g1, ..., gn)( [ p[1] ] ) -> transform [p[0],$] )
 * ==>
 *     e1 -> expand each p ( i=p[0], v=[p[1]], jump(i, g1(v), ..., gn(v)) -> transform [i,$] )
 * ==>
 *     e1 -> expand each p ( i=p[0], v=p[1], jump(i, g1([v]), ..., gn([v])) -> transform [i,$] )
 * ==>
 *     e1 -> expand each p ( i=p[0], v=p[1], jump(i, g1([v]), ..., gn([v])) -> tag(i) )
 * ==>
 *     e1 -> expand each p ( v=p[1], jump(p[0], g1([v]) -> tag(0), ..., gn([v]) -> tag(n)) )
 * ==>
 *     e1 -> expand each p jump( p[0], g1([p[1]]) -> tag(0), ..., gn([p[1]]) -> tag(n) )
 */
public class RetagExpand extends Rewrite
{
  public RetagExpand(RewritePhase phase)
  {
    super(phase, RetagFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr retag)
  {
    if(false) return false;
    assert retag instanceof RetagFn;
    int n = retag.numChildren() - 1;
    Expr input = retag.child(0);
    Expr expr;
    
    //  e1 -> retag( g1, ..., gN )
    // ==>
    //  e1 -> expand each p jump( p[0], g1([p[1]]) -> tag(0), ..., gn([p[1]]) -> tag(n) )

    Var pvar = engine.env.makeVar("p");
    
    // make args[i] = gi([p[1]]) -> tag(i)
    Expr[] gs = retag.children();
    Expr[] args = new Expr[n+1];
    for(int i = 1 ; i <= n ; i++)
    {
      expr = new IndexExpr(new VarExpr(pvar),1); // p[1]
      expr = new ArrayExpr(expr); // [p[1]]
      expr = new FunctionCallExpr(gs[i], expr); // gi([p[1]])
      expr = new TagFn(expr, new ConstExpr(i-1));
      args[i] = expr;
    }

    // make jump( p[0], gi()->tag(i)... )
    args[0] = new IndexExpr(new VarExpr(pvar),0); // p[0]
    expr = new JumpFn(args);
    
    // make: e1 -> expand each p (jump(...))
    expr = new ForExpr(pvar, input, expr);
    
    // replace retag by its definition
    retag.replaceInParent(expr);
    return true;
//
//    
//    Var pvar = engine.env.makeVar("p");
//    Var ivar = engine.env.makeVar("i");
//    Var vvar = engine.env.makeVar("v");
//
//    // make jump( i, g1([v]), ..., gn([v]) )
//    Expr[] gs = retag.children();
//    Expr[] args = new Expr[n+1];
//    args[0] = new VarExpr(ivar);
//    for(int i = 1 ; i <= n ; i++)
//    {
//      args[i] = new FunctionCallExpr(gs[i], new ArrayExpr(new VarExpr(vvar)));
//    }
//    Expr expr = new JumpFn(args);
//
////    // make: jump -> transform [i,$]
////    Var trVar = engine.env.makeVar("$");
////    expr = new TransformExpr(trVar, expr, new ArrayExpr(new VarExpr(ivar), new VarExpr(trVar)));
//    // make: jump -> tag(i)
//    expr = new TagFn(expr, new VarExpr(ivar));
//
//    // make (i=p[0], v=p[1], jump -> transform)
//    expr = new DoExpr(
//        new BindingExpr(BindingExpr.Type.EQ, ivar, null,
//            new IndexExpr(new VarExpr(pvar),0)),
//        new BindingExpr(BindingExpr.Type.EQ, vvar, null,
//            new IndexExpr(new VarExpr(pvar),1)),
//        expr);
//
//    // make: e1 -> expand each i (expr)
//    expr = new ForExpr(pvar, input, expr);
//
//    // replace retag by its definition
//    retag.replaceInParent(expr);
//    return true;
////   
////    // make jump( i[0], g1, ..., gn )
////    Expr[] args = new Expr[n+1];
////    System.arraycopy(retag.children(), 1, args, 1, n);
////    args[0] = new IndexExpr(new VarExpr(forVar), 0);
////    Expr expr = new JumpFn(args);
////
////    // make (jump)( [i[1]] )
////    expr = new FunctionCallExpr(expr, 
////        new ArrayExpr(new IndexExpr(new VarExpr(forVar), 1)));
////    
////    // make: expr -> transform [ i[0], $ ]
////    Var trVar = engine.env.makeVar("$");
////    expr = new TransformExpr(trVar, expr, 
////                 new ArrayExpr(
////                     new IndexExpr(new VarExpr(forVar), 0), 
////                     new VarExpr(trVar) ));
////    
////    // make: e1 -> expand each i (expr)
////    expr = new ForExpr(forVar, input, expr);
////
////    // replace retag by its definition
////    retag.replaceInParent(expr);
////    return true;
  }

}
