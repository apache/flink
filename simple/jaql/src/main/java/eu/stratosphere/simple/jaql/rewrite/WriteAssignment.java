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

import java.util.HashSet;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr;
import com.ibm.jaql.lang.expr.io.AbstractWriteExpr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * e0( e1( write( e2, e3 ) ) )
 * ==>
 * e0( ( v = write( e2, e3 ),
 *       e1( v ) ) )
 *   
 * where e2, e3 independent of e1, but not of e0
 */
public class WriteAssignment extends Rewrite // TODO: rename to Var inline
{
  /**
   * @param phase
   */
  @SuppressWarnings("unchecked")
  public WriteAssignment(RewritePhase phase)
  {
    super(phase, new Class[]{ AbstractWriteExpr.class, MapReduceBaseExpr.class }); // TODO: should fire on all writes
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr write)
  {
    // skip if already done inside a DoExpr
    Expr e = write.parent();
    if( e instanceof DoExpr ||
        ( e instanceof BindingExpr &&
          e.parent() instanceof DoExpr ) )
    {
      return false;
    }
    
    Expr safe = write;
    HashSet<Var> captures = write.getCapturedVars();
    // Find the first 'safe' ancestor of 'write' that:
    //   1. is the top of the query, or
    //   2. has a parent that might evaluate the subtree more than once, or
    //   3. defines a variable referenced inside the 'write' subtree, or
    findSafe: while( e != null )
    {
      if( e instanceof BindingExpr )
      {
        e = e.parent();
      }
      if( e instanceof ExplainExpr ||
          e instanceof QueryExpr ||
          e.evaluatesChildOnce(safe.getChildSlot()).maybeNot() )
      {
        break findSafe;
      }
      for( Var v: captures )
      {
        if( e.definesVar(v) )
        {
          break findSafe;
        }
      }
      safe = e;
      e = e.parent();
    }

    if( safe == write )
    {
      return false;
    }
    
    Var v = engine.env.makeVar("$fd_"+engine.counter(), write.getSchema()); // TODO: stash inline names to reuse here when possible
    write.replaceInParent(new VarExpr(v));
    BindingExpr bind = new BindingExpr(BindingExpr.Type.EQ, v, null, write);
    new DoExpr(bind, safe.injectAbove());
    return true;
  }
}
