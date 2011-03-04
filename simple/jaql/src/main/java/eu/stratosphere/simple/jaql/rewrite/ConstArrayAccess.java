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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
public class ConstArrayAccess extends Rewrite
{
  /**
   * @param phase
   */
  public ConstArrayAccess(RewritePhase phase)
  {
    super(phase, IndexExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    IndexExpr ie = (IndexExpr) expr;

    Expr array = ie.arrayExpr();
    
    if( array.getSchema().is(NULL).always() )
    {
      ie.replaceInParent(new ConstExpr(null));
      return true;
    }
    
    if (!(array instanceof ArrayExpr))
    {
      return false;
    }

    Expr index = ie.indexExpr();
    if (!(index instanceof ConstExpr))
    {
      return false;
    }

    Expr replaceBy;
    ConstExpr c = (ConstExpr) index;
    JsonLong li = (JsonLong) c.value;
    if (li == null)
    {
      replaceBy = c; // a[null] -> null
    }
    else
    {
      long i = li.get();
      if (i < 0 || i >= array.numChildren())
      {
        replaceBy = new ConstExpr(null);
      }
      else
      {
        replaceBy = array.child((int) i);
      }
    }
    ie.replaceInParent(replaceBy);
    return true;
  }
}
