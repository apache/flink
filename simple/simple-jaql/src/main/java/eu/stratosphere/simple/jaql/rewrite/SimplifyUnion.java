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
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Eliminate nested unions:
 *    union(e1,...,union(e2,...),e3,...)
 *    ==>
 *    union(e1,...,e2,...,e3,...)
 *     
 * If a union only has one leg, eliminate the union: 
 *    union(e1)
 *    ==>
 *    e1
 *    
 * When multiple legs of a union (merge) are ArrayExpr's put them into
 * a single ArrayExpr leg:   
 *    union(e1..., [e2,...], e3, [e4,...]) 
 *    ==>
 *    union(e1..., e3..., [e2,...,e4,...])
 *    
 * When a leg is empty, elminate it
 *    union(..., [], ...)
 *    ==>
 *    union(..., ...)
 * 
 */
public class SimplifyUnion extends Rewrite
{
  public SimplifyUnion(RewritePhase phase)
  {
    super(phase, UnionFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    UnionFn union = (UnionFn)expr;
    
    if( union.parent() instanceof UnionFn )
    {
      union.replaceInParent(union.children(), 0, union.numChildren());
      return true;
    }
    
    if( union.numChildren() == 1 )
    {
      union.replaceInParent(asArray(union.child(0)));
      return true;
    }
    
    
    Expr[] legs = union.children();
    int firstArray = -1;
    for(int i = 0 ; i < legs.length ; i++)
    {
      if( legs[i] instanceof ConstExpr )
      {
        // If leg is empty, remove it
        JsonValue val = ((ConstExpr)legs[i]).value;
        if( val == null )
        {
          // null is as good as empty
          legs[i].detach();
          return true;
        }
        else if( val instanceof JsonArray )
        {
          JsonArray arr = (JsonArray)val;
          long n = arr.count();
          if( n == 0 )
          {
            // empty constant array
            legs[i].detach();
            return true;
          }
          if( n < Integer.MAX_VALUE / 2 ) // don't merge huge arrays.
          {
            if( firstArray >= 0 )
            {
              mergeLegs(union, firstArray, i);
              return true;
            }
            firstArray = i;
          }
        }
      }
      else if( legs[i] instanceof ArrayExpr )
      {
        // If leg is empty, remove it
        if( legs[i].numChildren() == 0 )
        {
          legs[i].detach();
          return true;
        }
        if( firstArray >= 0 )
        {
          mergeLegs(union, firstArray, i);
          return true;
        }
        firstArray = i;
      }
    }
    
    return false;
  }
  
  protected void mergeLegs(UnionFn union, int i, int j) throws Exception
  {
    // Found a second ArrayExpr.  We will rewrite.
    // Split original legs into:
    //     newLegs = list of non-ArrayExpr
    //     arrayArgs = list of all ArrayExpr inputs
    Expr array1 = union.child(i);
    Expr array2 = union.child(j);
    array2.detach();
    if( array1 instanceof ConstExpr )
    {
      JsonArray arr1 = (JsonArray)((ConstExpr)array1).value;
      if( array2 instanceof ConstExpr )
      {
        JsonArray arr2 = (JsonArray)((ConstExpr)array2).value;
        SpilledJsonArray arr3 = new SpilledJsonArray();
        arr3.addCopyAll(arr1.iter());
        arr3.addCopyAll(arr2.iter());
        ((ConstExpr)array1).value = arr3;
      }
      else
      {
        mergeComputed(array1, (ArrayExpr)array2, arr1);
      }
    }
    else if( array2 instanceof ConstExpr )
    {
      JsonArray arr2 = (JsonArray)((ConstExpr)array2).value;
      mergeComputed(array1, (ArrayExpr)array1, arr2);
    }
    else
    {
      assert array1 instanceof ArrayExpr && array2 instanceof ArrayExpr;
      array1.addChildrenBefore(array1.numChildren(), array2.children());
    }
  }
  
  protected void mergeComputed(Expr old, ArrayExpr ae, JsonArray lit) throws Exception
  {
    assert lit.count() + ae.numChildren() < Integer.MAX_VALUE;
    int n = (int)lit.count();
    Expr[] exprs = new Expr[n + ae.numChildren()];
    for(int k = 0 ; k < n ; k++)
    {
      exprs[k] = new ConstExpr(lit.get(k));
    }
    System.arraycopy(ae.children(), 0, exprs, n, ae.numChildren());        
    old.replaceInParent(new ArrayExpr(exprs));
  }
}
