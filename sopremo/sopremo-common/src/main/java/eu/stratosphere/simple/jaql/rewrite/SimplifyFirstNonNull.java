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


import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.nil.FirstNonNullFn;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
public class SimplifyFirstNonNull extends Rewrite
{
  /**
   * @param phase
   */
  public SimplifyFirstNonNull(RewritePhase phase)
  {
    super(phase, FirstNonNullFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr fnn)
  {
    boolean modified = false;
    int last = fnn.numChildren() - 1;

    // remove any null inputs 
    int i;
    for (i = last; i >= 0; i--)
    {
      Expr c = fnn.child(i);
      if (c instanceof ConstExpr)
      {
        ConstExpr ce = (ConstExpr) c;
        if (ce.value == null)
        {
          modified = true;
          fnn.removeChild(i);
          last--;
        }
      }
    }

    // If we have no arguments, replace firstNonNull with null
    if (last < 0)
    {
      fnn.replaceInParent(new ConstExpr(null));
      return true;
    }

    // find the first non-nullable child that is not the last child
    for (i = 0; i < last; i++)
    {
      if (fnn.child(i).getSchema().is(NULL).never())
      {
        break;
      }
    }

    if (i == 0 || last == 0)
    {
      // if the first child is non-nullable or there is only one child
      // replace firstNonNull with the first child
      Expr c = fnn.child(i);
      fnn.replaceInParent(c);
      modified = true;
    }
    else
    {
      // remove all the items after the non-null child (if any)
      for (; last > i; last--)
      {
        fnn.removeChild(last);
        modified = true;
      }
    }

    return modified;
  }
}
