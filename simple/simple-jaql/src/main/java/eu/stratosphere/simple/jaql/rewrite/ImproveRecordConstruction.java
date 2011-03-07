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

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FixedRecordExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.rewrite.Rewrite;
import com.ibm.jaql.lang.rewrite.RewritePhase;


/**
 * Make performance improvements to record construction.
 * 
 * 1) Expand CopyRecord to CopyField when fields are known 
 * 2) Make optional NameValueBinding required when value is never null
 * 3) Make optional CopyField ALWAYS when field always exist value is never null
 *                            NONNULL when field always exist value but might be null
 * 4) Make RecordExpr into FixedRecordExpr when all fields are 
 *           NameValueBinding name is ConstExpr and required==true 
 *        or CopyField        name is ConstExpr and when==ALWAYS
 */
public class ImproveRecordConstruction extends Rewrite
{
  public ImproveRecordConstruction(RewritePhase phase)
  {
    super(phase, RecordExpr.class);
  }

  @Override
  public boolean rewrite(Expr expr)
  {
    RecordExpr rec = (RecordExpr) expr;
    
    if( rec instanceof FixedRecordExpr )
    {
      return false;
    }
    
    boolean modified = false;
    boolean isFixed = true;

    for( Expr e: rec.children() )
    {
      if( e instanceof NameValueBinding )
      {
        // Make optional NameValueBinding required when value is never null
        NameValueBinding f = (NameValueBinding)e;
        isFixed = isFixed && f.nameExpr() instanceof ConstExpr;
        if( ! f.isRequired() )
        {
          if( f.valueExpr().getSchema().is(JsonType.NULL).never() )
          {
            f.setRequired(true);
          }
          else
          {
            isFixed = false;
          }
        }
      }
      else if( e instanceof CopyField )
      {
        // Make optional CopyField ALWAYS when field always exist value is never null
        //                         NONNULL when field always exist value but might be null
        // Remove optional CopyField when the field never exists in the input record or
        //                           when the field is always null
        CopyField f = (CopyField)e;
        switch( f.getWhen() )
        {
          case ALWAYS:
            isFixed = isFixed && f.nameExpr() instanceof ConstExpr;
            break;
            
          case DEFINED:
          case NONNULL:
            boolean madeFixed = false;
            if( f.nameExpr() instanceof ConstExpr )
            {
              ConstExpr ce = (ConstExpr)f.nameExpr();
              JsonString fname = (JsonString)ce.value;
              Schema s = f.recExpr().getSchema();
              if( s instanceof RecordSchema )
              {
                RecordSchema rs = (RecordSchema)s;
                Field fs = rs.getField(fname);
                if( fs == null )
                {
                  if( ! rs.hasAdditional() )
                  {
                    // field cannot exist - remove it from the constructor
                    f.detach();
                    return true;
                  }
                }
                else if( ! fs.isOptional() )
                {
                  s = fs.getSchema();
                  switch( s.is(JsonType.NULL) )
                  {
                    case FALSE:
                      f.setWhen(CopyField.When.ALWAYS);
                      madeFixed = true;
                      break;
                    case TRUE:
                      // field cannot exist
                      f.detach();
                      return true;
                    case UNKNOWN:
                      f.setWhen(CopyField.When.NONNULL);
                      break;
                  }
                }
              }
            }
            isFixed = isFixed && madeFixed;
            break;
            
          default:
            assert false: "unknown case";
            isFixed = false;
        }
      }
      else if( e instanceof CopyRecord )
      {
        // Expand CopyRecord to CopyField when fields are known
        CopyRecord f = (CopyRecord)e;
        Expr re = f.recExpr();
        Schema s = re.getSchema();
        if( s instanceof RecordSchema )
        {
          RecordSchema rs = (RecordSchema)s;
          if( ! rs.hasAdditional() )
          {
            Var var;
            if( re instanceof VarExpr )
            {
              var = ((VarExpr)re).var();
            }
            else
            {
              var = engine.env.makeVar("crec");
            }
            Expr[] copyFields = new Expr[rs.noRequiredOrOptional()];
            for( int i = 0 ; i < copyFields.length ; i++ )
            {
              Field fs = rs.getFieldByPosition(i);
              copyFields[i] = new CopyField(new VarExpr(var), fs.getName(),
                      fs.isOptional() ? CopyField.When.NONNULL : CopyField.When.ALWAYS);
            }
            f.replaceInParent(copyFields, 0, copyFields.length);
            if( !(re instanceof VarExpr) )
            {
              Expr bind = new BindingExpr(BindingExpr.Type.EQ, var, null, re);
              new DoExpr(bind, rec.injectAbove());
            }
            return true;
          }
        }
        isFixed = false;
      }
      else
      {
        // Unknown field constructor....
        return isFixed = false;
      }
    }
    
    if( !isFixed )
    {
      return modified;
    }

    // Make RecordExpr into FixedRecordExpr when all fields are 
    //         NameValueBinding name is ConstExpr and required==true 
    //      or CopyField        name is ConstExpr and when==ALWAYS

    rec.replaceInParent(new FixedRecordExpr(rec.children()));
    return true;
  }
}
