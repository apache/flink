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

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.pragma.InlinePragma;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * 
 */
public class DoInlinePragma extends Rewrite
{
  /**
   * @param phase
   */
  public DoInlinePragma(RewritePhase phase)
  {
    super(phase, InlinePragma.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    assert expr instanceof InlinePragma;
    Expr c = expr.child(0);
    if (c instanceof VarExpr)
    {
      VarExpr varExpr = (VarExpr) c;
      Var var = varExpr.var();
      Expr def = varExpr.findVarDef();
//    // TODO: I think this case is gone now; function parameters are in bindings that are found.
//      if (def == null)
//      {
//        // must be a function parameter // TODO: findVarDef SHOULD find it, and params should use Bindings
//        return false;
//      }
      if (def instanceof BindingExpr)
      {
        BindingExpr b = (BindingExpr)def;
        assert var == b.var; // or else findDef is broken
        Expr p = def.parent();
        if( p instanceof DoExpr )
        {
          expr.replaceInParent(cloneExpr(b.eqExpr()));
          return true;
        }
        //else if( p instanceof DefineFunctionExpr )
        // We couldn't inline, yet...
        return false;
      }
      else // TODO: I don't think this case arises anymore...
      {
        assert false;
        return false;
//        assert var.isGlobal();
//        Expr replaceBy;
//        if (var.value != null)
//        {
//          // If the global is already computed, inline its value
//          replaceBy = new ConstExpr(var.value);
//        }
//        else
//        {
//          // If the global is not already computed, inline its expr
//          replaceBy = cloneExpr(def);
//        }
//        expr.replaceInParent(replaceBy);
//        return true;
      }
    }
    // If this inline request is not over a VarExpr, just remove it.
    expr.replaceInParent(c);
    return true;
  }
}
