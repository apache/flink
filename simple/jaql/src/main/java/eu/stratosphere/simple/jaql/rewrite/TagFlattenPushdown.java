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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.JumpFn;
import com.ibm.jaql.lang.expr.core.TagFlattenFn;
import com.ibm.jaql.lang.expr.core.TagFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Push TagFlatten down the tree
 * 
 * 1: e0 -> expand e1 -> tagFlatten(i,k)
 *    ==>
 *    e0 -> expand (e1 -> tagFlatten(i,k))
 * 
 * 2: merge(e1,..., eN) -> tagFlatten(i,k)
 *    ==>
 *    merge(e1 -> tagFlatten(i,k),..., eN -> tagFlatten(i,k))
 * 
 * 3: e1 -> tag(i) -> tagFlatten(j,k)
 *    ==>
 *    assert i >= 0, k >= 0
 *    when i<j:  e1 -> tag(i)
 *    when i>j:  when k=1: e1 -> tag(i)
 *                    k>1: e1 -> tag(i+k-1)
 *    when i=j:  when i=0: e1 
 *                    i>0: e1 -> tagPlus(j)     (tagPlus(j) == tagFlatten(-1,j+1))
 * 
 * 4: e1 -> jump(i,e1,...,eN) -> tagFlatten(j,k) -> e3
 *    ==>
 *    e1 -> jump(i, e1 -> tagFlatten(j,k), ..., eN -> tagFlatten(j,k)) -> e3
 */
public class TagFlattenPushdown extends Rewrite
{
  public TagFlattenPushdown(RewritePhase phase)
  {
    super(phase, TagFlattenFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr flat)
  {
    assert flat instanceof TagFlattenFn;
    Expr input = flat.child(0);
    if( input instanceof ForExpr )
    {
      // push inside the loop
      // e0 -> expand e1 -> tagFlatten(i,k) -> e2
      // ==>
      // e0 -> expand (e1 -> tagFlatten(i,k)) -> e2
      ForExpr fe = (ForExpr)input;
      flat.replaceInParent(fe);
      flat.setChild(0, fe.collectExpr());
      fe.setChild(1, flat);
      return true;
    }
    else if( input instanceof UnionFn )
    {
      // merge(e1,..., eN) -> tagFlatten(i,k) -> e3
      // ==>
      // merge(e1 -> tagFlatten(i,k),..., eN -> tagFlatten(i,k)) -> e3
      replicateToEveryLeg(flat, input, 0);
      return true;
    }
    else if( input instanceof TagFn )
    {
      // e1 -> tag(i) -> tagFlatten(j,k) -> e2
      Expr replaceBy;
      Expr ei = input.child(1);
      Expr ej = flat.child(1);
      Expr ek = flat.child(2);
      if( ei instanceof ConstExpr &&
          ej instanceof ConstExpr &&
          ek instanceof ConstExpr )
      {
        long i = ((JsonNumber)((ConstExpr)ei).value).longValueExact();
        long j = ((JsonNumber)((ConstExpr)ej).value).longValueExact();
        long k = ((JsonNumber)((ConstExpr)ek).value).longValueExact();
        if( i < 0 )
        {
          throw new RuntimeException("invalid tag: "+i);
        }
        if( k < 0 )
        {
          throw new RuntimeException("invalid tag size: "+k);
        }
        // if j < 0 then we are adding k-1 to every index (ie, tagPlus(k-1))
        if( i != j ) 
        {
          // e1 -> tag(a), a = if(i>j && k+1) i+k-1 else i
          replaceBy = input;
          if( i > j && k != 1 )
          {
            ((ConstExpr)ei).value = new JsonLong(i+k-1);
          }
        }
        else 
        {
          replaceBy = input.child(0); // e1
          // if i=j=0: e1 -> tagPlus(0) == e1
          if( j > 0 ) // i=j>0: e1 -> tagPlus(j) == e1 -> tagFlatten(-1,j+1)
          {
            // TODO: remove TagPlusFn: replaceBy = new TagPlusFn(replaceBy,ej);
            replaceBy = new TagFlattenFn(replaceBy,
                new ConstExpr(JsonLong.MINUS_ONE),new ConstExpr(j+1));
          }
        }
        flat.replaceInParent(replaceBy);
        return true;
      }
    }
    else if( input instanceof JumpFn )
    {
      //    e1 -> jump(i,e1,...,eN) -> tagFlatten(j,k) -> e3
      // ==>
      //    e1 -> jump(i, e1 -> tagFlatten(j,k), ..., eN -> tagFlatten(j,k))
      replicateToEveryLeg(flat, input, 1);
      return true;
    }
    return false;
  }
  
  protected void replicateToEveryLeg(Expr flat, Expr input, int startChild)
  {
    flat.replaceInParent(input);
    Expr index = flat.child(1);
    Expr size = flat.child(2);
    int n = input.numChildren();
    for( int i = startChild ; i < n ; i++)
    {
      Expr leg = input.child(i);
      leg = new TagFlattenFn(leg, cloneExpr(index), cloneExpr(size));
      input.setChild(i, leg);
    }
  }

}
