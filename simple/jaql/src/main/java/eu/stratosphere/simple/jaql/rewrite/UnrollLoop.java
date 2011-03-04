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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.pragma.UnrollLoopPragma;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;


public abstract class UnrollLoop extends Rewrite
{
  public UnrollLoop(RewritePhase phase, Class<? extends Expr> fireOn)
  {
    super(phase, fireOn);
  }

  /**
   * Do most of the work to unroll a loop (ForExpr or TransformExpr).
   * Returns null if the loop cannot be unrolled
   *         the list of unrolled expressions otherwise.
   */
  protected Expr[] buildExprList(BindingExpr binding, Expr returnExpr) throws Exception
  {
    // FIXME: do we need to restrict the returnExprs we can handle? Is it ok to clone side-effecting fns, eg?
    // The same problem exists with inlining function variables (which makes extra copies).
    // One bad case is: 
    //    [1,2,3] -> randomLong(17)
    // which produces 3 different numbers starting with seed 17, but:
    //    [randomLong(17),randomLong(17),randomLong(17)]
    // produces the same random number starting at seed 17 three times
    // because the function maintains some call-site state.
    // This could also mess up performance of some operators, eg:
    //    [1,2,3] -> transform R(...)
    // will have one R interpreter started, but the inline version will start three interpreters.
    // TODO: add another property that blocks unrolling?  Something like SENSITIVE_TO_NUMBER_OF_CALL_SITES?
    // TODO: For now we will only fire this rule when we have a pragma to force it.  We could get more agressive.
    Expr[] inList = null;
    boolean hasPragma = binding.parent().parent() instanceof UnrollLoopPragma;    
    Expr inExpr = binding.inExpr();    
    
    if( inExpr instanceof ArrayExpr )
    {
      if( hasPragma || inExpr.numChildren() <= 1 )
      {
        inList = inExpr.children();
      }
    }
    else if( inExpr instanceof ConstExpr )
    {
      ConstExpr ce = (ConstExpr)inExpr;
      JsonArray arr = (JsonArray)ce.value;
      if( arr == null )
      {
        return Expr.NO_EXPRS;
      }
      else
      {
        int n =(int)arr.count();
        if( hasPragma || n <= 1 )
        {
          inList = new Expr[n];
          n = 0;
          for(JsonValue v: arr)
          {
            inList[n++] = new ConstExpr(v);
          }
        }
      }
    }
// Let const eval get the ConstExpr to us. 
//    else if( inExpr.isCompileTimeComputable().always() )
//    {
//      JsonArray arr = (JsonArray)inExpr.compileTimeEval();
//      int n =(int)arr.count();
//      if( arr == null )...
//      inList = new Expr[n];
//      n = 0;
//      for(JsonValue v: arr)
//      {
//        inList[n++] = new ConstExpr(v);
//      }
//    }
    
    if( inList == null )
    {
      return null;
    }
    
    Expr[] exprs = new Expr[inList.length];
    int i = 0;
    Var var = binding.var;
    VarMap varMap = engine.varMap;
    
    for( Expr e: inList )
    {
      varMap.clear();
      Var newVar = var.clone(varMap);
      varMap.put(var, newVar);
      exprs[i++] =
        new DoExpr(
            new BindingExpr(BindingExpr.Type.EQ, newVar, null, e), 
            returnExpr.clone(varMap) );
    }
    return exprs;
  }
}
