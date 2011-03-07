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

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.ToArrayFn;
import com.ibm.jaql.lang.expr.core.AggregateExpr;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;

/**
 * toArray( array ) ==> array
 * toArray( nonarray ) ==> [ nonarray ]
 */
public class ToArrayElimination extends Rewrite
{
  /**
   * @param phase
   */
  public ToArrayElimination(RewritePhase phase)
  {
    super(phase, ToArrayFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    assert expr instanceof ToArrayFn;
    Expr input = expr.child(0);
    
    // TODO: remove this block
    if( input instanceof VarExpr )  
    {
      // TODO: A Var should know it's schema properties instead of looking for certain definitions. 
      VarExpr ve = (VarExpr)input;
      Var v = ve.var();
      Expr def = ve.findVarDef();
      if( def instanceof BindingExpr )
      {
        Expr p = def.parent();
        if( p instanceof GroupByExpr )
        {
          GroupByExpr g = (GroupByExpr)p;
          for(int i = 0 ; i < g.numInputs() ; i++)
          {
            if( g.getAsVar(i) == v )
            {
              expr.replaceInParent(input);
              return true;
            }
          }
        }
        else if( p instanceof AggregateExpr )
        {
          assert def == p.child(0); // must be the input
          expr.replaceInParent(input);
          return true;
        }
      }
    }
    
    Bool3 isArrayOrNull = input.getSchema().is(ARRAY, NULL);
    if( isArrayOrNull.always() )
    {
      expr.replaceInParent(input);
      return true;
    }
    else if( isArrayOrNull.never() )
    {
      expr.replaceInParent(new ArrayExpr(input));
      return true;
    }
    
    return false;
  }
}
