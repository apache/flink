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
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TagSplitFn;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefineJaqlFunctionExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.expr.io.HadoopTempExpr;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Expand tagSplit a write to a composite file:
 *     e1 -> tagSplit( g1, ..., gN )
 * ==>
 *    ( f1 = ..., ..., 
 *      fN = ...,
 *      e1 -> write( composite( [f1, ..., fN] ) ),
 *      h1(f1), ...
 *      hN(fN) )
 *      
 *  where    
 *     if gi = -> write(f) -> more() then
 *         fi = f
 *         hi = -> more() 
 *     else 
 *        fi = HadoopTemp()
 *        hi = -> read() -> gi
 *   
 * ==> //TODO: should really be this, but need to fix up isMapReducible to dig through the path expression
 *    ( fd = e1 -> write( composite( [f1, ..., fN] ) ),
 *      fd.descriptors[0] -> h1(), ...
 *      fd.descriptors[N-1] -> hN() )
 */
public class SplitToWrite extends Rewrite
{
  public SplitToWrite(RewritePhase phase)
  {
    super(phase, TagSplitFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr split)
  {
    assert split instanceof TagSplitFn;
    int n = split.numChildren() - 1;
    Expr input = split.child(0);

    // FIXME: add schema inference into temps.
    // FIXME: we need a way to preserve schemas through tagging
    Schema schema = SchemaFactory.anySchema();
    
    Expr[] doLegs = new Expr[2*n+1];
    Expr[] fds = new Expr[n];
    for(int i = 1 ; i <= n ; i++)
    {
      Var fi = engine.env.makeVar("f"+i);
      Expr fd = null;
      Expr hi = null;
      Expr e = split.child(i);
      if( e instanceof DefineJaqlFunctionExpr )
      {
        DefineJaqlFunctionExpr fndef = (DefineJaqlFunctionExpr)e;
        if( fndef.numParams() == 1 )
        {
          Var param = fndef.varOf(0);
          Expr body = fndef.body();
          if( countVarUse(body, param) == 1 )
          {
            VarExpr use = findFirstVarUse(body, param);
            if( use.parent() instanceof WriteFn )
            {
              WriteFn write = (WriteFn)use.parent();
              if( write.isMapReducible() )
              {
                fd = write.descriptor();
                hi = new VarExpr(fi);
                if( write != body )
                {
                  write.replaceInParent(hi);
                  hi = body;
                }
                // TODO: else omit this doLeg; it is a no-op
              }
            }
          }
        }
      }
      
      if( fd == null )
      {
        fd = new HadoopTempExpr(schema);
        hi = new FunctionCallExpr(e, new ReadFn(new VarExpr(fi)));
      }
      
      doLegs[i-1] = new BindingExpr(BindingExpr.Type.EQ, fi, null, fd);
      fds[i-1] = new VarExpr(fi);
      doLegs[n+i] = hi;
    }
    
    doLegs[n] = new WriteFn(input, CompositeOutputAdapter.makeDescriptor(fds) );
    Expr doExpr = new DoExpr(doLegs);
    split.replaceInParent(doExpr);
    
    return true;
  }

}
