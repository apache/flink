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

import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DiamondTagFn;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.RetagFn;
import com.ibm.jaql.lang.expr.core.TagFlattenFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * 1. Compose two retags or a retag of a diamondTag into one:
 * 
 *     e1 -> foo( f1, ..., fN ) -> retag( g1, ..., gN )
 *         where foo is either retag or diamondTag
 * ==>
 *     e1 -> foo( fn(x) g1(f1(x)), ...., fn(x) gN(fN(x)) )
 *     
 * 2. Compose retag of a tagFlatten
 * 
 *     e1 -> tagFlatten(j,k) -> retag( f1, ..., fN )
 *            where j,k are constants and j >= 0 and k > 0
 * ==>
 *     e1 -> retag( f1, ..., fj-1, 
 *                  fj= fn(x) retag(x, fj, ..., fj+k-1), 
 *                  fj+k, ..., fN )
 *        -> tagFlatten(j,k)
 */
public class RetagMerge extends Rewrite
{
  public RetagMerge(RewritePhase phase)
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
    assert retag instanceof RetagFn;
    int n = retag.numChildren();
    Expr input = retag.child(0);
    
    if( ( input instanceof RetagFn || 
          input instanceof DiamondTagFn ) &&
        input.numChildren() == n )
    {
      // Replace each fi with hi = fn(x) g(f(x))
      for( int i = 1 ; i < n ; i++ )
      {
        Var x = engine.env.makeVar("x");
        Expr f = input.child(i);
        Expr g = retag.child(i);
        Expr h = 
          new DefineJaqlFunctionExpr(new Var[]{x}, 
              new FunctionCallExpr(g, 
                  new FunctionCallExpr(f, new VarExpr(x))));
        input.setChild(i, h);
      }
      // remove the original retag
      retag.replaceInParent(input);
      return true;
    }
    else if( input instanceof TagFlattenFn )
    {
      // e1 -> tagFlatten(j,k) -> retag( f1, ..., fN )
      // ==>
      // e1 -> retag( f1, ..., fj-1,
      //              fj= fn(x) retag(x, fj, ..., fj+k-1),
      //              fj+k, ..., fN )
      //    -> tagFlatten(j,k)
      Expr flat = input;
      Expr ej = flat.child(1);
      Expr ek = flat.child(2);
      if( ej instanceof ConstExpr &&
          ek instanceof ConstExpr )
      {
        int j = ((JsonNumber)((ConstExpr)ej).value).intValueExact();
        int k = ((JsonNumber)((ConstExpr)ek).value).intValueExact();
        if( j >= 0 && k > 0 )
        {
          // we will rewrite
          if( j < n - 1 )
          {
            // if not, then the tagFlatten shouldn't be happening...
            // build: fn(x) retag(x, fj, ..., fj+k-1)
            Expr[] oldargs = retag.children();
            Var xvar = engine.env.makeVar("x");
            Expr[] args = new Expr[k+1];
            args[0] = new VarExpr(xvar);
            System.arraycopy(oldargs, j+1, args, 1, k);
            Expr expr = new RetagFn(args);
            expr = new DefineJaqlFunctionExpr(new Var[]{xvar}, expr);
            // replace args in old retag
            args = new Expr[n-k+1];
            System.arraycopy(oldargs, 1, args, 1, j);
            System.arraycopy(oldargs, j+k+1, args, j+2, n-(j+k+1));
            args[0] = flat.child(0);
            args[j+1] = expr;
            retag.setChildren(args);
            // move tagFlatten after retag
            retag.replaceInParent(flat);
            flat.setChild(0, retag);
            return true;
          }
        }
      }
    }

    return false;
  }

}
