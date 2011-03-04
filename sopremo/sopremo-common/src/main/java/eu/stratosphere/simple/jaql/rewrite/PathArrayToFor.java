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

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.SliceFn;
import com.ibm.jaql.lang.expr.array.ToArrayFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathArray;
import com.ibm.jaql.lang.expr.path.PathArrayAll;
import com.ibm.jaql.lang.expr.path.PathArrayHead;
import com.ibm.jaql.lang.expr.path.PathArraySlice;
import com.ibm.jaql.lang.expr.path.PathArrayTail;
import com.ibm.jaql.lang.expr.path.PathExpand;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.lang.expr.path.PathToArray;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;
/**
 * e [*] p ==> for( $i in         e  ) [ $i p ]
 * e [?] p ==> for( $i in toArray(e) ) [ $i p ]
 * e []  p ==> for( $i in toArray(e) ) toArray( $i p )
 */
public class PathArrayToFor extends Rewrite
{
  /**
   * @param phase
   */
  public PathArrayToFor(RewritePhase phase)
  {
    super(phase, PathArray.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    if( !( expr.parent() instanceof PathExpr ) )
    {
      return false;
    }

    PathExpr pe = (PathExpr)expr.parent();
    PathStep nextStep = ((PathArray)expr).nextStep();
    Expr outer = pe.input();
    Schema outerSchema = outer.getSchema();
    // FIXME: (kbeyer) What is the problem here? It is preventing combiners in map/reduce jobs... 
    // if (outerSchema.isArray().maybeNot()) return false; // otherwise problems with schema inference
    Schema elements = outerSchema.elements();
    if( elements == null )
    {
      // The var schema really should be null because it will never be set to a value.
      // But makeVar raises an exception in this case.
      // FIXME: (kbeyer) remove assertion?
      // We could also avoid generating the loop when outerSchema.is(ARRAY,NULL).always() but no elements.
      elements = SchemaFactory.anySchema();
    }
    Var v = engine.env.makeVar("$", elements);
    Expr inner = new VarExpr(v);
    
    if( expr instanceof PathArrayAll ||
        expr instanceof PathToArray ||
        expr instanceof PathExpand )
    {
      if( ! outerSchema.is(ARRAY,NULL).always() &&
          ( expr instanceof PathToArray ||
            expr instanceof PathExpand ) )
      {
        outer = new ToArrayFn(outer);
      }

      if( !( nextStep instanceof PathReturn ) )
      {
        inner =  new PathExpr( inner, nextStep );
      }

      if( expr instanceof PathArrayAll ||
          expr instanceof PathToArray )
      {
        inner = new ArrayExpr(inner);
      }
      else if( ! nextStep.getSchema().is(ARRAY,NULL).always() )
      {
        assert expr instanceof PathExpand;
        inner = new ToArrayFn(inner);
      }

    }
    else 
    {
      Expr low;
      Expr high;
      if( expr instanceof PathArrayHead )
      {
        low  = new ConstExpr(null);
        high = ((PathArrayHead)expr).lastIndex();
      }
      else if( expr instanceof PathArraySlice )
      {
        low  = ((PathArraySlice)expr).firstIndex();
        high = ((PathArraySlice)expr).lastIndex();
      }
      else if( expr instanceof PathArrayTail )
      {
        low  = ((PathArrayTail)expr).firstIndex();
        high  = new ConstExpr(null);
      }
      else
      {
        return false; // We shouldn't get here unless someone adds a new PathArray subclass
      }
      outer = new SliceFn(outer, low, high);
      if( nextStep instanceof PathReturn )
      {
        pe.replaceInParent(outer);
        return true;
      }
      inner = new PathExpr(inner, nextStep);
    }
    
    ForExpr fe = new ForExpr(v, outer, inner);
    pe.replaceInParent(fe);
    return true;
  }
}
