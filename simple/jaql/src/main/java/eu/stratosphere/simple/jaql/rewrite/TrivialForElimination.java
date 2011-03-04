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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * for $i in e collect ([] | null) ==> []
 * 
 * for $i in ([] | null) collect e ==> []
 * 
 * for $i in e collect [$i] ==> asArray(e)
 * 
 * e1 -> expand [e2] ==> e1 -> transform e2
 * 
 */
public class TrivialForElimination extends Rewrite
{
  /**
   * @param phase
   */
  public TrivialForElimination(RewritePhase phase)
  {
    super(phase, ForExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    ForExpr fe = (ForExpr) expr;
    Expr inExpr = fe.binding().inExpr();
    Expr c = fe.collectExpr();

    // for( i in []   ) e    => []
    // for( i in null ) e    => []
    // for( i in e    ) []   => []
    // for( i in e    ) null => []
    // TODO: was this, but it ran slowly: if (inExpr.getSchema().isEmpty(ARRAY,NULL).always() || c.getSchema().isEmpty(ARRAY,NULL).always())
    if ( isEmptyArrayOrNull(inExpr) ||
         isEmptyArrayOrNull(c) )
    {
      fe.replaceInParent(new ArrayExpr());
      return true;
    }

    // look for collect [$i]
    if (!(c instanceof ArrayExpr) || c.numChildren() != 1)
    {
      return false;
    }    
    c = c.child(0);
    
    // for( $i in e1 ) [$i] => asArray(e1) => e1 (when non-null array)
    if (c instanceof VarExpr)
    {
      VarExpr ve = (VarExpr) c;
      if (ve.var() == fe.var())
      {
        if (inExpr.getSchema().is(ARRAY,NULL).maybeNot() || inExpr.getSchema().is(NULL).maybe())
        {
          inExpr = new AsArrayFn(inExpr);
        }
        fe.replaceInParent(inExpr);
        return true;
      }
    }
    
    // e1 -> expand [e2] ==> e1 -> transform e2
    expr = new TransformExpr(fe.binding(), c);
    fe.replaceInParent(expr);
    return true;
  }
  
  public static boolean isEmptyArrayOrNull(Expr expr)
  {
    if( expr instanceof ConstExpr )
    {
      JsonValue v = ((ConstExpr)expr).value;
      if( v == null )
      {
        return true;
      }
      else if( v instanceof JsonArray )
      {
        return ((JsonArray)v).isEmpty();
      }
    }
    else if( expr instanceof ArrayExpr )
    {
      return ((ArrayExpr)expr).numChildren() == 0;
    }
    return false;
  }
}
