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

import java.util.Map.Entry;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.CopyField.When;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;

/**
 * Simplify record construction
 * 
 * { { n1:e,... }.*, n2:e,... }
 * ==>
 * { n1:e, ..., n2:e, ... }
 *   
 */
// TODO: should { {x:1,y:2}.*, x:3 } == {y:2, x:3} ?
// TODO: handle CopyField
public class SimplifyRecord extends Rewrite
{
  public SimplifyRecord(RewritePhase phase)
  {
    super(phase, RecordExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    RecordExpr target = (RecordExpr)expr;
    
    // Find a copied record constructor or constant record
    for( Expr e: target.children() )
    {
      if( e instanceof NameValueBinding )
      {
        NameValueBinding nv = (NameValueBinding)e;
        if( !(nv.nameExpr() instanceof ConstExpr) )
        {
          // If we have computed field names, we will not combine records.
          // TODO: This is conservative, in case we make literal names override any copied fields.
          return false;
        }
      }
      else if( e instanceof CopyField )
      {
        CopyField f = (CopyField)e;
        Expr ne = f.nameExpr();
        if( !(ne instanceof ConstExpr) )
        {
          // If we have computed field names, we will not combine records.
          // TODO: This is conservative, in case we make literal names override any copied fields.
          return false;
        }
        JsonString name = (JsonString)((ConstExpr)ne).value;
        if( name == null )
        {
          throw new RuntimeException("null field names are not allowed:\n"+expr);
        }
        e = f.recExpr();
        if( e instanceof RecordExpr )
        {
          e = ((RecordExpr)e).findStaticFieldValue(name);
          // TODO: handle the case when name definitely does NOT exist
          if( e != null )
          {
            f.replaceInParent( new NameValueBinding(name, e, f.getWhen() != When.NONNULL) );
            return true;
          }
        }
        else if( e instanceof ConstExpr )
        {
          JsonValue val = ((ConstExpr)e).value;
          if( val == null )
          {
            if( f.getWhen() != When.ALWAYS )
            {
              f.detach();
              return true;
            }
          }
          else if( val instanceof JsonRecord )
          {
            JsonRecord rec = (JsonRecord)val;
            val = rec.get(name);
          }
          else
          {
            throw new ClassCastException("record is required in "+f+" found "+val);
          }
          
          f.replaceInParent( new NameValueBinding(name, new ConstExpr(val), true) );
          return true;
        }
      }
      else if( e instanceof CopyRecord )
      {
        CopyRecord f = (CopyRecord)e;
        e = f.recExpr();
        if( e instanceof RecordExpr )
        {
          return copyRecordConstructor(target, (RecordExpr)e);
        }
        else if( e instanceof ConstExpr )
        {
          return copyConstRecord(target, (ConstExpr)e);
        }
      }
    }
    return false;
  }
  
  protected boolean copyRecordConstructor(RecordExpr target, RecordExpr toCopy)
  {
    // Check that all literal names in rec are statically known and not in the parent record.
    // This is to be conservative, in case we make literal names override any copied fields.
    for( Expr c: toCopy.children() )
    {
      if( c instanceof NameValueBinding )
      {
        NameValueBinding nv = (NameValueBinding)c;
        if( !(nv.nameExpr() instanceof ConstExpr) )
        {
          return false;
        }
        ConstExpr ce = (ConstExpr)nv.nameExpr();
        JsonString copyName = (JsonString)ce.value;
        if( copyName == null )
        {
          throw new RuntimeException("null field names are not allowed:\n"+toCopy);
        }
        checkDuplicateNames(target, copyName);
      }
    }
    
    // Promote FieldExpr's from toCopy record into target.
    Expr[] fieldExprs = toCopy.children();
    toCopy.parent().replaceInParent(fieldExprs, 0, fieldExprs.length);
    return true;
  }

  protected boolean copyConstRecord(RecordExpr target, ConstExpr toCopy)
  {
    JsonRecord rec = (JsonRecord)toCopy.value;
    if( rec == null )
    {
      // { (null).*, ... } ==> { ... }
      toCopy.parent().detach();
      return true;
    }
    
    // Check that all literal names in rec are statically known and not in the parent record.
    // This is to be conservative, in case we make literal names override any copied fields.
    for( Entry<JsonString, JsonValue> nameValue: rec )
    {
      checkDuplicateNames(target, nameValue.getKey());
    }
    
    // Promote fields from rec into target
    Expr[] fieldExprs = new Expr[rec.size()];
    int i = 0;
    for( Entry<JsonString, JsonValue> nameValue: rec )
    {
      fieldExprs[i++] = new NameValueBinding(
          new ConstExpr(nameValue.getKey()), 
          new ConstExpr(nameValue.getValue()),
          true);
    }
    toCopy.parent().replaceInParent(fieldExprs, 0, fieldExprs.length);
    return true;
  }
  
  protected void checkDuplicateNames(RecordExpr target, JsonString copyName)
  {
    for( Expr e: target.children() )
    {
      if( e instanceof NameValueBinding )
      {
        NameValueBinding nv = (NameValueBinding)e;
        ConstExpr ce = (ConstExpr)nv.nameExpr();
        JsonString name = (JsonString)ce.value;
        if( name == null )
        {
          throw new RuntimeException("null field names are not allowed:\n"+target);
        }
        if( name.equals(copyName) )
        {
          // TODO: this might be allowed soon
          throw new RuntimeException("duplicate field names are (yet?) not allowed:\n"+target);
        }
      }
    }
  }
}
