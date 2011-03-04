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

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

// TODO: This rewrite possibly go away with the change in FOR definition to preserve input.

/**
 * asArray(null) ==> []
 * asArray(nonnull array e) ==> e
 * asArray(nullable array e) ==> emptyOnNull(e)
 * expr(asArray(e)) ==> expr(e) for expr that has asArray built-in.
 */
public class AsArrayElimination extends Rewrite
{
  /**
   * @param phase
   */
  public AsArrayElimination(RewritePhase phase)
  {
    super(phase, AsArrayFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    Expr input = expr.child(0);
    
    Schema schema = input.getSchema();

    // asArray(null) -> []
    if (schema.is(NULL).always())
    {
      expr.replaceInParent(new ArrayExpr());
      return true;
    }

    // asArray( non-nullable array expr ) -> expr
    if (schema.is(ARRAY).always())
    {
      expr.replaceInParent(input);
      return true;
    }
    
    //   for ($i in asArray(e)) ...
    // | asArray(e) -> transform ...
    // | group $i in asArray ...
    // =>
    // eliminate asArray
    // TODO: should just ask expr.parent() if it consumes an array in this arg.
    if (expr.parent() instanceof BindingExpr)
    {
      Expr gp = expr.parent().parent();
      if (gp instanceof ForExpr 
          || gp instanceof TransformExpr
          || ( gp instanceof GroupByExpr 
              && expr.parent() == ((GroupByExpr)gp).inBinding() ) )
      {
        expr.replaceInParent(input);
        return true;
      }
    }
    
    // for($i in e1) asArray(e2) ==>  for($i in e1) e2
    if( expr.parent() instanceof ForExpr )
    {
      expr.replaceInParent(input);
      return true;
    }

    if (schema.is(ARRAY,NULL).always() )
    {
      expr.replaceInParent(new EmptyOnNullFn(input));
      return true;
    }

    // asArray( if t then e1 else e2 ) -> if t then asArray(e1) else asArray(e2)
    if (input instanceof IfExpr)
    {
      IfExpr ifExpr = (IfExpr) input;
      expr.replaceInParent(ifExpr); // remove asArray

      Expr e = ifExpr.trueExpr();
      schema = e.getSchema();
      if (schema.is(NULL).always())
      {
        e.replaceInParent(new ArrayExpr());
      }
      else if (!(schema.is(ARRAY).always()))
      {
        ifExpr.setChild(1, new AsArrayFn(e));
      }

      e = ifExpr.falseExpr();
      schema = e.getSchema();
      if (schema.is(NULL).always())
      {
        e.replaceInParent(new ArrayExpr());
      }
      else if (!(schema.is(ARRAY).always()))
      {
        ifExpr.setChild(2, new AsArrayFn(e));
      }

      return true;
    }

    return false;
  }
}
