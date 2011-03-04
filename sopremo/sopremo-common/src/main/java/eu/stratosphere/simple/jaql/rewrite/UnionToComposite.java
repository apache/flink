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

import com.ibm.jaql.io.hadoop.CompositeOutputAdapter;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.core.JumpFn;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Open up parallelism for union:
 * 
 *    union( read(A) -> ma*, ..., read(B) -> mb* )
 *  ==>
 *    read( composite([A,B]) ) -> retag( -> ma, -> mb ) -> detag()
 * 
 *    where A,...,B are mappable inputs
 *          ma,...,mb are mappable expressions
 */
public class UnionToComposite extends Rewrite
{
  public UnionToComposite(RewritePhase phase)
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

    int n = union.numChildren();
    if( n <= 1 )
    {
      return false;
    }

    // Check that every leg is read -> mappable*
    int identity = 0;
    for( Expr e: union.children() )
    {
      Expr s = getMappableSource(e);
      if( !( s instanceof ReadFn ) )
      {
        return false;
      }
      ReadFn read = (ReadFn)s;
      if( ! read.isMapReducible() )
      {
        return false;
      }
      if( s == e )
      {
        identity++;
      }
    }

    // Every leg is read -> mappable*
    // Rewrite!
    //
    // If all legs are simply reads:
    //
    //    read( composite(fds) )
    //     -> transform $[1]
    //
    // Else:
    //
    //    read( composite(fds) ) 
    //     -> expand jump( $[0], [$[1]] -> ma, ..., [$[1]] -> mb )
    //

    // fds[] = all the read descriptors
    // maps[] = all the mappable expressions placed into a lambda
    //          maps[0] will be the input
    Expr[] fds = new Expr[n];
    boolean jumping = n != identity;
    Expr[] maps = jumping ? new Expr[1 + n] : null;
    Var var = engine.env.makeVar("$");
    
    for( int i = 0 ; i < n ; i++ )
    {
      Expr e = union.child(i);
      ReadFn read = (ReadFn)getMappableSource(e);
      fds[i] = read.descriptor();
      if( jumping )
      {
        Expr valExpr = new ArrayExpr(new IndexExpr(new VarExpr(var), 1));
        read.replaceInParent(valExpr);
        if( read == e )
        {
          e = valExpr;
        }
        maps[i+1] = e;
      }
    }

    Expr replaceBy = new ReadFn(CompositeOutputAdapter.makeDescriptor(fds));
    if( jumping )
    {
      // read -> expand jump( $[0], $[1] -> ma, ..., $[1] -> mb )
      maps[0] = new IndexExpr(new VarExpr(var), 0); 
      replaceBy = new ForExpr(var, replaceBy, new JumpFn(maps));
    }
    else
    {
      // read -> transform $[1]
      replaceBy = new TransformExpr(var, replaceBy, new IndexExpr(new VarExpr(var), 1));
    }
    
    union.replaceInParent(replaceBy);
    
    return true;
  }
}
