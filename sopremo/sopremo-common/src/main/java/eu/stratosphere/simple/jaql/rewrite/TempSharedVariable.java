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

import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.io.hadoop.CompositeOutputAdapter;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

//FIXME: currently unused in favor of special handling of tee.  This might come back to life.


/**
 * Replace a variable that is mappable to a distributed read and
 * any mappable uses of that variable with a temporary file.
 * This is done after we are finished inlining the variables and
 * before we make mapReduce. 
 *
 * ( v = read(mr) -> m0*, v -> mi* -> ei, ... )
 * ==>
 * ( fi = ...,
 *   ...
 *   read(mr) -> m0* -> expand merge( [$] -> mi -> transform [i,$],... ) -> write( {[fi,...]} ), 
 *   read(fi) -> ei,
 *   ... )
 * 
 * where: read(mr) is map-reducible input
 *        m0 is a sequence of zero or more mappable expressions
 *        mi is a sequence of zero or more mappable expressions
 *           plus the first map-reducible write(fi) if it exists 
 * then:  
 *        {[fi,...]} is a composite file descriptor  
 *        fi is either the descriptor from the write() we found, 
 *           or HadoopTemp() if there wasn't a write.
 * however:
 *        all uses of v that do not have any mi share the same fi = HadoopTemp()
 *
 * weaknesses: // TODO: fix these problems...
 * 
 *    o We could share more complex common subexpressions (CSEs).  For example:
 *    
 *                       /- m4                 ( x1 = read -> m1,  
 *               /- m2 -+                        x2 = x1 -> m2,
 *              /        \- m5                   x4 = x2 -> m4,
 *         m1 -+                                 x5 = x2 -> m5,
 *              \        /- m6                   x3 = x1 -> m3,
 *               \- m3 -+                        x6 = x3 -> m6,
 *                       \- m7                   x7 = x3 -> m7 )
 * 
 *     This could be performed in a single map task with four outputs. 
 *     But the way this code works, it will bundle ( m1, m2, m3 ) into a single stream,
 *     and temp the result.  Then it will bundle (m4,m5) and (m6,m7) into two
 *     more streams.  We end up with three map/reduce jobs where we should have 
 *     produced only one.
 *     
 *   o We should avoid definining fi and referencing it twice (in the write and the read)
 *     because we prefer a dataflow version that flows the write to the read.
 *     However, we are unable to see the map-reducible descriptors when we have this:
 *         read( compFd.descriptors[i] )
 *     Either the isMapReducible logic needs to be improved, or we need another way
 *     to find distributed inputs and outputs.
 *     The main problem is that our dataflow analysis wont see the file dependencies today.
 *     
 *   o Like LetInline, this code does not respect read -> write conflicts:
 *     it might move a write of a file before a read of that same file.
 *     (LetInline does the opposite: a read might be moved after a write.)
 *
 */
public class TempSharedVariable extends Rewrite
{
  /**
   * @param phase
   */
  public TempSharedVariable(RewritePhase phase)
  {
    super(phase, DoExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    DoExpr doExpr = (DoExpr) expr;

    assert !(doExpr.returnExpr() instanceof BindingExpr); // do expr never ends with a binding

    // Find the first variable binding for an array
    int n = doExpr.numChildren() - 1;
    BindingExpr bind = null;
    ArrayList<Expr> uses = engine.exprList;
    int i;
    for( i = 0 ; i < n ; i++ )
    {
      Expr e = doExpr.child(i);
      if( e instanceof BindingExpr )
      {
        // Move through a sequence of mappable expressions
        for( e = ((BindingExpr)e).eqExpr() ; e.isMappable(0) ; e = e.child(0) )
        {
          if( e.child(0) instanceof BindingExpr )
          {
            e = e.child(0);
          }
        }
        if( e instanceof ReadFn )
        {
          ReadFn read = (ReadFn)e;
          if( read.isMapReducible() )
          {
            // we found: v = read(mr) -> m*
            bind = (BindingExpr)doExpr.child(i);
            
            // Find all the uses of v
            uses.clear();
            for( int j = i + 1 ; j <= n ; j++ )
            {
              doExpr.child(j).getVarUses(bind.var, uses);
            }
            if( uses.size() >= 2 )
            {
              // v has multiple uses (usually preventing LetInline), rewrite it
              // to use a shared file instead of a variable.
              // TODO: should we also rewrite when there is fewer than 2 uses?
              // I think these cases will be handled by LetInline or ToMapReduce. (ksb)
              break;
            }
          }
        }
      }
    }
    
    if( i >= n )
    {
      // we didn't find v = read(mr) -> m*
      return false;
    }
    
    // We will rewrite!
    assert uses.size() >= 2;

    // Get the set of variables that are safe to move around
    HashSet<Var> safeVars = doExpr.getCapturedVars();
    for(int j = 0 ; j <= i ; j++)
    {
      Expr e = doExpr.child(j);
      if( e instanceof BindingExpr )
      {
        safeVars.add( ((BindingExpr)e).var );
      }
    }

    // Find the mapping expressions (mi) over each variable use
    int numWithMap = 0;
    int numUses = uses.size();
    Expr[] maps = new Expr[numUses];
    for(int j = 0 ; j < numUses ; j++)
    {
      Expr e = uses.get(j).parent();
      if( e instanceof BindingExpr &&
          e.getChildSlot() == 0 )
      {
        e = e.parent();
      }
      // We combine merge mappable expressions that don't:
      //     o have side-effects: we don't want to pull up a side-effect before it's time 
      //     o reference undefined variables 
      while( e.isMappable(0) && 
             e.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never() && // TODO: should just check children > 0 or place into isMappable
             ! e.hasCaptures(safeVars) )
      {
        maps[j] = e;
        e = e.parent();
        if( e instanceof BindingExpr &&
            e.getChildSlot() == 0 )
        {
          e = e.parent();
        }
      }
      if( e instanceof WriteFn &&
          ((WriteFn)e).isMapReducible() )
      {
        maps[j] = e;
      }
      if( maps[j] != null )
      {
        numWithMap++;
      }
    }
    
    // Find or create the file descriptors, create the union legs, and
    //
    // replace                 with            where fds[f] is           union leg is
    // ---------------------   ------------    ------------------------  -----------------------------
    // v -> mi* -> write(fd)   fds[f]          this fd                   [$] -> mi* -> transform [f,$]
    // v -> mi+                read(fds[f])    a new temp                [$] -> mi+ -> transform [f,$] 
    // v                       read(fds[last]) a shared emp (fds[last])  [$] -> transform [f,$]
    //
    // (..., v_fd = ... -> write( composite([...]) )
    Var forVar = engine.env.makeVar( bind.var.name() + "_t");
    int m = Math.min(numWithMap+1, numUses);
    Expr[] unionLegs = new Expr[m];
    Expr[] writeFds = new Expr[m];
    BindingExpr[] fdDefs = new BindingExpr[m];
    int f = 0;
    Var lastFd = null;
    for( int j = 0 ; j < numUses ; j++ )
    {
      Expr use = uses.get(j);
      Expr map = maps[j];
      Expr afterWrite;
      Expr replaceParent;
      int replaceSlot;

      if( map != null )
      {
        replaceParent = map.parent();
        replaceSlot = map.getChildSlot();
        Var fdVar = engine.env.makeVar("tfd_"+f);
        Var transformVar = engine.env.makeVar("ti_"+f);
        afterWrite = new VarExpr(fdVar);
        use.replaceInParent(new ArrayExpr( new VarExpr(forVar) ));
        Expr fd;
        Expr data;
        if( map instanceof WriteFn )
        {
          WriteFn write = (WriteFn)map;
          fd = write.descriptor();
          data = write.dataExpr();
        }
        else
        {
          fd = new HadoopTempExpr(SchemaFactory.anySchema()); // FIXME: add schema: lastExpr.getSchema().elements();
          data = map;
          afterWrite = new ReadFn( afterWrite );
        }
        fdDefs[f] = new BindingExpr(BindingExpr.Type.EQ, fdVar, null, fd);
        writeFds[f] = new VarExpr(fdVar);
        unionLegs[f] =
          new TransformExpr(transformVar,
              data,
              new ArrayExpr( new ConstExpr(f), new VarExpr(transformVar) ));
        f++;
      }
      else
      {
        replaceParent = use.parent();
        replaceSlot = use.getChildSlot();
        // The single leg & fd shared among all non-mapping uses.
        assert numWithMap < numUses;
        if( lastFd == null ) // only create for the first of the non-mapping uses.
        {
          lastFd = engine.env.makeVar("tfd_"+numWithMap);
          Var transformVar = engine.env.makeVar("ti_"+numWithMap);
          // FIXME: add schema to HadoopTempExpr: lastExpr.getSchema().elements();
          fdDefs[numWithMap] = new BindingExpr(BindingExpr.Type.EQ, lastFd, null, new HadoopTempExpr(SchemaFactory.anySchema()));
          writeFds[numWithMap] = new VarExpr(lastFd);
          unionLegs[numWithMap] =
            new TransformExpr(transformVar, 
                new ArrayExpr( new VarExpr(forVar) ), 
                new ArrayExpr( new ConstExpr(numWithMap), new VarExpr(transformVar) ));
        }
        afterWrite = new ReadFn( new VarExpr(lastFd) );
      }
      
      if( replaceParent == doExpr &&
          replaceSlot < doExpr.numChildren() - 1 )
      {
        // The result after the map is not used, so remove it
        // (..., map, ..., last) ==> (..., ..., last)
        doExpr.removeChild(replaceSlot);
      }
      else
      {
        replaceParent.setChild(replaceSlot, afterWrite);
      }
    }
    
    // build
    //   ... -> expand each $forVar merge( unionLegs ) -> write( composite([...]) ) 
    //
    // Make the expanded union
    Expr e = new UnionFn(unionLegs);
    // Add the mapping expressions to the shared variable binding expression
    e = new ForExpr(forVar, bind.eqExpr(), e);
    // Write to a composite output
    e = new WriteFn( e, CompositeOutputAdapter.makeDescriptor(writeFds) );
    
    // Replace the variable definition with the write()
    doExpr.setChild(i, e);
    // Add the fd definitions
    doExpr.addChildrenBefore(i, fdDefs);
    
    return true;
  }
}
