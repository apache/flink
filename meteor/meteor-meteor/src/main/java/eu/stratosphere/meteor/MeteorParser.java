// $ANTLR 3.3 Nov 30, 2010 12:46:29 /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g 2012-07-19 22:21:02
 
package eu.stratosphere.meteor; 

import eu.stratosphere.sopremo.*;
import eu.stratosphere.sopremo.query.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.type.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import eu.stratosphere.sopremo.function.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
import java.util.IdentityHashMap;
import java.util.Arrays;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

public class MeteorParser extends AbstractQueryParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "ID", "VAR", "STRING", "STAR", "DECIMAL", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "APOSTROPHE", "QUOTATION", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'using'", "'='", "'fn'", "'('", "','", "')'", "'javaudf'", "'?'", "':'", "'if'", "'or'", "'||'", "'and'", "'&&'", "'not'", "'in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'as'", "'.'", "'{'", "'}'", "'true'", "'false'", "'null'", "'['", "']'", "'read'", "'from'", "'write'", "'to'", "'preserve'"
    };
    public static final int EOF=-1;
    public static final int T__26=26;
    public static final int T__27=27;
    public static final int T__28=28;
    public static final int T__29=29;
    public static final int T__30=30;
    public static final int T__31=31;
    public static final int T__32=32;
    public static final int T__33=33;
    public static final int T__34=34;
    public static final int T__35=35;
    public static final int T__36=36;
    public static final int T__37=37;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int T__44=44;
    public static final int T__45=45;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int T__50=50;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__59=59;
    public static final int T__60=60;
    public static final int T__61=61;
    public static final int T__62=62;
    public static final int T__63=63;
    public static final int T__64=64;
    public static final int T__65=65;
    public static final int T__66=66;
    public static final int T__67=67;
    public static final int T__68=68;
    public static final int T__69=69;
    public static final int EXPRESSION=4;
    public static final int OPERATOR=5;
    public static final int ID=6;
    public static final int VAR=7;
    public static final int STRING=8;
    public static final int STAR=9;
    public static final int DECIMAL=10;
    public static final int INTEGER=11;
    public static final int UINT=12;
    public static final int LOWER_LETTER=13;
    public static final int UPPER_LETTER=14;
    public static final int DIGIT=15;
    public static final int SIGN=16;
    public static final int COMMENT=17;
    public static final int APOSTROPHE=18;
    public static final int QUOTATION=19;
    public static final int WS=20;
    public static final int UNICODE_ESC=21;
    public static final int OCTAL_ESC=22;
    public static final int ESC_SEQ=23;
    public static final int HEX_DIGIT=24;
    public static final int EXPONENT=25;

    // delegates
    // delegators


        public MeteorParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public MeteorParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
            this.state.ruleMemo = new HashMap[117+1];
             
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return MeteorParser.tokenNames; }
    public String getGrammarFileName() { return "/Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g"; }


    {
      addTypeAlias("int", IntNode.class);
      addTypeAlias("decimal", DecimalNode.class);
      addTypeAlias("string", TextNode.class);
      addTypeAlias("double", DoubleNode.class);
      addTypeAlias("boolean", BooleanNode.class);
      addTypeAlias("bool", BooleanNode.class);
      
      addParserFlag(ParserFlag.FUNCTION_OBJECTS);
    }

    public void parseSinks() throws RecognitionException {  
        script();
    }

    private boolean setInnerOutput(Token VAR, Operator<?> op) {
      JsonStreamExpression output = new JsonStreamExpression(((operator_scope)operator_stack.peek()).result.getOutput(((objectCreation_scope)objectCreation_stack.peek()).mappings.size()));
      ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping(output, new JsonStreamExpression(op)));
      setBinding(VAR, output, 1);
      return true;
    }

    private String getAssignmentName(EvaluationExpression expression) {
      if(expression instanceof PathExpression)
        return getAssignmentName(((PathExpression) expression).getLastFragment());
      if(expression instanceof ObjectAccess)
        return ((ObjectAccess) expression).getField();
      return expression.toString();
    }

    private EvaluationExpression makePath(Token inputVar, String... path) {
      Object input = getRawBinding(inputVar, Object.class);
      if(input instanceof Operator<?>) {
        int inputIndex = ((operator_scope)operator_stack.peek()).result.getInputs().indexOf(((Operator<?>)input).getSource());
        input = new InputSelection(inputIndex);
      } else if(input instanceof JsonStreamExpression)
        input = ((JsonStreamExpression)input).toInputSelection(((operator_scope)operator_stack.peek()).result);
      
      List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
      accesses.add((EvaluationExpression) input);
      for (String fragment : path)
        accesses.add(new ObjectAccess(fragment));
      return PathExpression.wrapIfNecessary(accesses);
    }


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:87:1: script : ( statement ';' )+ ->;
    public final MeteorParser.script_return script() throws RecognitionException {
        MeteorParser.script_return retval = new MeteorParser.script_return();
        retval.start = input.LT(1);
        int script_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal2=null;
        MeteorParser.statement_return statement1 = null;


        EvaluationExpression char_literal2_tree=null;
        RewriteRuleTokenStream stream_26=new RewriteRuleTokenStream(adaptor,"token 26");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 1) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:88:2: ( ( statement ';' )+ ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:88:5: ( statement ';' )+
            {
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:88:5: ( statement ';' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>=ID && LA1_0<=VAR)||LA1_0==27||LA1_0==65||LA1_0==67) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:88:6: statement ';'
            	    {
            	    pushFollow(FOLLOW_statement_in_script125);
            	    statement1=statement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            	    char_literal2=(Token)match(input,26,FOLLOW_26_in_script127); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_26.add(char_literal2);


            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 88:22: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 1, script_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "script"

    public static class statement_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "statement"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:90:1: statement : ( assignment | operator | packageImport | functionDefinition | javaudf ) ->;
    public final MeteorParser.statement_return statement() throws RecognitionException {
        MeteorParser.statement_return retval = new MeteorParser.statement_return();
        retval.start = input.LT(1);
        int statement_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.assignment_return assignment3 = null;

        MeteorParser.operator_return operator4 = null;

        MeteorParser.packageImport_return packageImport5 = null;

        MeteorParser.functionDefinition_return functionDefinition6 = null;

        MeteorParser.javaudf_return javaudf7 = null;


        RewriteRuleSubtreeStream stream_assignment=new RewriteRuleSubtreeStream(adaptor,"rule assignment");
        RewriteRuleSubtreeStream stream_functionDefinition=new RewriteRuleSubtreeStream(adaptor,"rule functionDefinition");
        RewriteRuleSubtreeStream stream_javaudf=new RewriteRuleSubtreeStream(adaptor,"rule javaudf");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        RewriteRuleSubtreeStream stream_packageImport=new RewriteRuleSubtreeStream(adaptor,"rule packageImport");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:2: ( ( assignment | operator | packageImport | functionDefinition | javaudf ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            {
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            int alt2=5;
            switch ( input.LA(1) ) {
            case VAR:
                {
                alt2=1;
                }
                break;
            case 65:
            case 67:
                {
                alt2=2;
                }
                break;
            case ID:
                {
                int LA2_3 = input.LA(2);

                if ( (LA2_3==28) ) {
                    int LA2_5 = input.LA(3);

                    if ( (LA2_5==29) ) {
                        alt2=4;
                    }
                    else if ( (LA2_5==33) ) {
                        alt2=5;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 2, 5, input);

                        throw nvae;
                    }
                }
                else if ( ((LA2_3>=ID && LA2_3<=VAR)||LA2_3==63||LA2_3==69) ) {
                    alt2=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 3, input);

                    throw nvae;
                }
                }
                break;
            case 27:
                {
                alt2=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }

            switch (alt2) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:5: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement141);
                    assignment3=assignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_assignment.add(assignment3.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:18: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement145);
                    operator4=operator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_operator.add(operator4.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:29: packageImport
                    {
                    pushFollow(FOLLOW_packageImport_in_statement149);
                    packageImport5=packageImport();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_packageImport.add(packageImport5.getTree());

                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:45: functionDefinition
                    {
                    pushFollow(FOLLOW_functionDefinition_in_statement153);
                    functionDefinition6=functionDefinition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_functionDefinition.add(functionDefinition6.getTree());

                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:91:66: javaudf
                    {
                    pushFollow(FOLLOW_javaudf_in_statement157);
                    javaudf7=javaudf();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_javaudf.add(javaudf7.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 91:75: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 2, statement_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "statement"

    public static class packageImport_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "packageImport"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:93:1: packageImport : 'using' packageName= ID ->;
    public final MeteorParser.packageImport_return packageImport() throws RecognitionException {
        MeteorParser.packageImport_return retval = new MeteorParser.packageImport_return();
        retval.start = input.LT(1);
        int packageImport_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token string_literal8=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression string_literal8_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:94:3: ( 'using' packageName= ID ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:94:6: 'using' packageName= ID
            {
            string_literal8=(Token)match(input,27,FOLLOW_27_in_packageImport172); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_27.add(string_literal8);

            packageName=(Token)match(input,ID,FOLLOW_ID_in_packageImport176); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(packageName);

            if ( state.backtracking==0 ) {
               importPackage((packageName!=null?packageName.getText():null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 94:66: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 3, packageImport_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "packageImport"

    public static class assignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "assignment"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:96:1: assignment : target= VAR '=' source= operator ->;
    public final MeteorParser.assignment_return assignment() throws RecognitionException {
        MeteorParser.assignment_return retval = new MeteorParser.assignment_return();
        retval.start = input.LT(1);
        int assignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token target=null;
        Token char_literal9=null;
        MeteorParser.operator_return source = null;


        EvaluationExpression target_tree=null;
        EvaluationExpression char_literal9_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:97:2: (target= VAR '=' source= operator ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:97:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment191); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal9=(Token)match(input,28,FOLLOW_28_in_assignment193); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal9);

            pushFollow(FOLLOW_operator_in_assignment197);
            source=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(source.getTree());
            if ( state.backtracking==0 ) {
               setBinding(target, new JsonStreamExpression((source!=null?source.op:null))); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 97:98: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 4, assignment_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "assignment"

    public static class functionDefinition_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionDefinition"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:99:1: functionDefinition : name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->;
    public final MeteorParser.functionDefinition_return functionDefinition() throws RecognitionException {
        MeteorParser.functionDefinition_return retval = new MeteorParser.functionDefinition_return();
        retval.start = input.LT(1);
        int functionDefinition_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token param=null;
        Token char_literal10=null;
        Token string_literal11=null;
        Token char_literal12=null;
        Token char_literal13=null;
        Token char_literal14=null;
        MeteorParser.contextAwareExpression_return def = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression param_tree=null;
        EvaluationExpression char_literal10_tree=null;
        EvaluationExpression string_literal11_tree=null;
        EvaluationExpression char_literal12_tree=null;
        EvaluationExpression char_literal13_tree=null;
        EvaluationExpression char_literal14_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
         List<Token> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:3: (name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:5: name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition219); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal10=(Token)match(input,28,FOLLOW_28_in_functionDefinition221); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal10);

            string_literal11=(Token)match(input,29,FOLLOW_29_in_functionDefinition223); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_29.add(string_literal11);

            char_literal12=(Token)match(input,30,FOLLOW_30_in_functionDefinition225); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal12);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:102:3: (param= ID ( ',' param= ID )* )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==ID) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:102:4: param= ID ( ',' param= ID )*
                    {
                    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition234); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(param);

                    if ( state.backtracking==0 ) {
                       params.add(param); 
                    }
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:3: ( ',' param= ID )*
                    loop3:
                    do {
                        int alt3=2;
                        int LA3_0 = input.LA(1);

                        if ( (LA3_0==31) ) {
                            alt3=1;
                        }


                        switch (alt3) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:4: ',' param= ID
                    	    {
                    	    char_literal13=(Token)match(input,31,FOLLOW_31_in_functionDefinition241); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal13);

                    	    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition245); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(param);

                    	    if ( state.backtracking==0 ) {
                    	       params.add(param); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop3;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal14=(Token)match(input,32,FOLLOW_32_in_functionDefinition256); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal14);

            if ( state.backtracking==0 ) {
               for(int index = 0; index < params.size(); index++) setBinding(params.get(index), new InputSelection(0)); 
            }
            pushFollow(FOLLOW_contextAwareExpression_in_functionDefinition268);
            def=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(def.getTree());
            if ( state.backtracking==0 ) {
               addFunction(new SopremoFunction(name.getText(), def.tree)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 106:100: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 5, functionDefinition_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "functionDefinition"

    public static class javaudf_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "javaudf"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:108:1: javaudf : name= ID '=' 'javaudf' '(' path= STRING ')' ->;
    public final MeteorParser.javaudf_return javaudf() throws RecognitionException {
        MeteorParser.javaudf_return retval = new MeteorParser.javaudf_return();
        retval.start = input.LT(1);
        int javaudf_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token path=null;
        Token char_literal15=null;
        Token string_literal16=null;
        Token char_literal17=null;
        Token char_literal18=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression path_tree=null;
        EvaluationExpression char_literal15_tree=null;
        EvaluationExpression string_literal16_tree=null;
        EvaluationExpression char_literal17_tree=null;
        EvaluationExpression char_literal18_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:109:3: (name= ID '=' 'javaudf' '(' path= STRING ')' ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:109:5: name= ID '=' 'javaudf' '(' path= STRING ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_javaudf286); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal15=(Token)match(input,28,FOLLOW_28_in_javaudf288); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal15);

            string_literal16=(Token)match(input,33,FOLLOW_33_in_javaudf290); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_33.add(string_literal16);

            char_literal17=(Token)match(input,30,FOLLOW_30_in_javaudf292); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal17);

            path=(Token)match(input,STRING,FOLLOW_STRING_in_javaudf296); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_STRING.add(path);

            char_literal18=(Token)match(input,32,FOLLOW_32_in_javaudf298); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal18);

            if ( state.backtracking==0 ) {
               addFunction(name.getText(), path.getText()); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 110:53: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 6, javaudf_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "javaudf"

    protected static class contextAwareExpression_scope {
        EvaluationExpression context;
    }
    protected Stack contextAwareExpression_stack = new Stack();

    public static class contextAwareExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwareExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:112:1: contextAwareExpression[EvaluationExpression contextExpression] : expression ;
    public final MeteorParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        MeteorParser.contextAwareExpression_return retval = new MeteorParser.contextAwareExpression_return();
        retval.start = input.LT(1);
        int contextAwareExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.expression_return expression19 = null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:3: ( expression )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:5: expression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_expression_in_contextAwareExpression326);
            expression19=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression19.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 7, contextAwareExpression_StartIndex); }
            contextAwareExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "contextAwareExpression"

    public static class expression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:117:1: expression : ( ternaryExpression | operatorExpression );
    public final MeteorParser.expression_return expression() throws RecognitionException {
        MeteorParser.expression_return retval = new MeteorParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.ternaryExpression_return ternaryExpression20 = null;

        MeteorParser.operatorExpression_return operatorExpression21 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:118:3: ( ternaryExpression | operatorExpression )
            int alt5=2;
            alt5 = dfa5.predict(input);
            switch (alt5) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:118:5: ternaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_ternaryExpression_in_expression336);
                    ternaryExpression20=ternaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, ternaryExpression20.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:119:5: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_expression342);
                    operatorExpression21=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression21.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 8, expression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class ternaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "ternaryExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:121:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );
    public final MeteorParser.ternaryExpression_return ternaryExpression() throws RecognitionException {
        MeteorParser.ternaryExpression_return retval = new MeteorParser.ternaryExpression_return();
        retval.start = input.LT(1);
        int ternaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal22=null;
        Token char_literal23=null;
        Token string_literal24=null;
        MeteorParser.orExpression_return ifClause = null;

        MeteorParser.expression_return ifExpr = null;

        MeteorParser.expression_return elseExpr = null;

        MeteorParser.orExpression_return ifExpr2 = null;

        MeteorParser.expression_return ifClause2 = null;

        MeteorParser.orExpression_return orExpression25 = null;


        EvaluationExpression char_literal22_tree=null;
        EvaluationExpression char_literal23_tree=null;
        EvaluationExpression string_literal24_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_36=new RewriteRuleTokenStream(adaptor,"token 36");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_orExpression=new RewriteRuleSubtreeStream(adaptor,"rule orExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:2: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression )
            int alt7=3;
            alt7 = dfa7.predict(input);
            switch (alt7) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression353);
                    ifClause=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifClause.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:27: '?' (ifExpr= expression )? ':' elseExpr= expression
                    {
                    char_literal22=(Token)match(input,34,FOLLOW_34_in_ternaryExpression356); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_34.add(char_literal22);

                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:37: (ifExpr= expression )?
                    int alt6=2;
                    int LA6_0 = input.LA(1);

                    if ( ((LA6_0>=ID && LA6_0<=STRING)||(LA6_0>=DECIMAL && LA6_0<=INTEGER)||LA6_0==30||(LA6_0>=52 && LA6_0<=55)||LA6_0==58||(LA6_0>=60 && LA6_0<=63)||LA6_0==65||LA6_0==67) ) {
                        alt6=1;
                    }
                    switch (alt6) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ifExpr= expression
                            {
                            pushFollow(FOLLOW_expression_in_ternaryExpression360);
                            ifExpr=expression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_expression.add(ifExpr.getTree());

                            }
                            break;

                    }

                    char_literal23=(Token)match(input,35,FOLLOW_35_in_ternaryExpression363); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal23);

                    pushFollow(FOLLOW_expression_in_ternaryExpression367);
                    elseExpr=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(elseExpr.getTree());

                    }



                    // AST REWRITE
                    // elements: ifClause
                    // token labels: 
                    // rule labels: retval, ifClause
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause=new RewriteRuleSubtreeStream(adaptor,"rule ifClause",ifClause!=null?ifClause.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 123:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:123:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause.nextTree());
                        adaptor.addChild(root_1,  ifExpr == null ? EvaluationExpression.VALUE : (ifExpr!=null?((EvaluationExpression)ifExpr.tree):null) );
                        adaptor.addChild(root_1,  (elseExpr!=null?((EvaluationExpression)elseExpr.tree):null) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:124:4: ifExpr2= orExpression 'if' ifClause2= expression
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression390);
                    ifExpr2=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifExpr2.getTree());
                    string_literal24=(Token)match(input,36,FOLLOW_36_in_ternaryExpression392); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_36.add(string_literal24);

                    pushFollow(FOLLOW_expression_in_ternaryExpression396);
                    ifClause2=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(ifClause2.getTree());


                    // AST REWRITE
                    // elements: ifClause2, ifExpr2
                    // token labels: 
                    // rule labels: retval, ifExpr2, ifClause2
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifExpr2=new RewriteRuleSubtreeStream(adaptor,"rule ifExpr2",ifExpr2!=null?ifExpr2.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause2=new RewriteRuleSubtreeStream(adaptor,"rule ifClause2",ifClause2!=null?ifClause2.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 125:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:125:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause2.nextTree());
                        adaptor.addChild(root_1, stream_ifExpr2.nextTree());
                        adaptor.addChild(root_1,  EvaluationExpression.VALUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:126:5: orExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_orExpression_in_ternaryExpression418);
                    orExpression25=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression25.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 9, ternaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "ternaryExpression"

    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:128:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? ->;
    public final MeteorParser.orExpression_return orExpression() throws RecognitionException {
        MeteorParser.orExpression_return retval = new MeteorParser.orExpression_return();
        retval.start = input.LT(1);
        int orExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal26=null;
        Token string_literal27=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal26_tree=null;
        EvaluationExpression string_literal27_tree=null;
        RewriteRuleTokenStream stream_37=new RewriteRuleTokenStream(adaptor,"token 37");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression431);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:26: ( ( 'or' | '||' ) exprs+= andExpression )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( ((LA9_0>=37 && LA9_0<=38)) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:27: ( 'or' | '||' )
            	    int alt8=2;
            	    int LA8_0 = input.LA(1);

            	    if ( (LA8_0==37) ) {
            	        alt8=1;
            	    }
            	    else if ( (LA8_0==38) ) {
            	        alt8=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 8, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt8) {
            	        case 1 :
            	            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:28: 'or'
            	            {
            	            string_literal26=(Token)match(input,37,FOLLOW_37_in_orExpression435); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_37.add(string_literal26);


            	            }
            	            break;
            	        case 2 :
            	            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:129:35: '||'
            	            {
            	            string_literal27=(Token)match(input,38,FOLLOW_38_in_orExpression439); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_38.add(string_literal27);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression444);
            	    exprs=andExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop9;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 130:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 131:3: ->
            {
                adaptor.addChild(root_0,  OrExpression.valueOf(list_exprs) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 10, orExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "orExpression"

    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "andExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:133:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? ->;
    public final MeteorParser.andExpression_return andExpression() throws RecognitionException {
        MeteorParser.andExpression_return retval = new MeteorParser.andExpression_return();
        retval.start = input.LT(1);
        int andExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal28=null;
        Token string_literal29=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal28_tree=null;
        EvaluationExpression string_literal29_tree=null;
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression473);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( ((LA11_0>=39 && LA11_0<=40)) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:31: ( 'and' | '&&' )
            	    int alt10=2;
            	    int LA10_0 = input.LA(1);

            	    if ( (LA10_0==39) ) {
            	        alt10=1;
            	    }
            	    else if ( (LA10_0==40) ) {
            	        alt10=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 10, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt10) {
            	        case 1 :
            	            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:32: 'and'
            	            {
            	            string_literal28=(Token)match(input,39,FOLLOW_39_in_andExpression477); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_39.add(string_literal28);


            	            }
            	            break;
            	        case 2 :
            	            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:134:40: '&&'
            	            {
            	            string_literal29=(Token)match(input,40,FOLLOW_40_in_andExpression481); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_40.add(string_literal29);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression486);
            	    exprs=elementExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop11;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 135:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 136:3: ->
            {
                adaptor.addChild(root_0,  AndExpression.valueOf(list_exprs) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 11, andExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "andExpression"

    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "elementExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:138:1: elementExpression : elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) ;
    public final MeteorParser.elementExpression_return elementExpression() throws RecognitionException {
        MeteorParser.elementExpression_return retval = new MeteorParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token not=null;
        Token string_literal30=null;
        MeteorParser.comparisonExpression_return elem = null;

        MeteorParser.comparisonExpression_return set = null;


        EvaluationExpression not_tree=null;
        EvaluationExpression string_literal30_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:2: (elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:4: elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )?
            {
            pushFollow(FOLLOW_comparisonExpression_in_elementExpression515);
            elem=comparisonExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:30: ( (not= 'not' )? 'in' set= comparisonExpression )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( ((LA13_0>=41 && LA13_0<=42)) ) {
                alt13=1;
            }
            switch (alt13) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:31: (not= 'not' )? 'in' set= comparisonExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:139:34: (not= 'not' )?
                    int alt12=2;
                    int LA12_0 = input.LA(1);

                    if ( (LA12_0==41) ) {
                        alt12=1;
                    }
                    switch (alt12) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: not= 'not'
                            {
                            not=(Token)match(input,41,FOLLOW_41_in_elementExpression520); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(not);


                            }
                            break;

                    }

                    string_literal30=(Token)match(input,42,FOLLOW_42_in_elementExpression523); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal30);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression527);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: elem, elem, set
            // token labels: 
            // rule labels: elem, retval, set
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_elem=new RewriteRuleSubtreeStream(adaptor,"rule elem",elem!=null?elem.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_set=new RewriteRuleSubtreeStream(adaptor,"rule set",set!=null?set.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 140:2: -> { set == null }? $elem
            if ( set == null ) {
                adaptor.addChild(root_0, stream_elem.nextTree());

            }
            else // 141:2: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
            {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:141:5: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ElementInSetExpression"), root_1);

                adaptor.addChild(root_1, stream_elem.nextTree());
                adaptor.addChild(root_1,  not == null ? ElementInSetExpression.Quantor.EXISTS_IN : ElementInSetExpression.Quantor.EXISTS_NOT_IN);
                adaptor.addChild(root_1, stream_set.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 12, elementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "elementExpression"

    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:144:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
    public final MeteorParser.comparisonExpression_return comparisonExpression() throws RecognitionException {
        MeteorParser.comparisonExpression_return retval = new MeteorParser.comparisonExpression_return();
        retval.start = input.LT(1);
        int comparisonExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.arithmeticExpression_return e1 = null;

        MeteorParser.arithmeticExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression568);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( ((LA15_0>=43 && LA15_0<=48)) ) {
                alt15=1;
            }
            switch (alt15) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
                    int alt14=6;
                    switch ( input.LA(1) ) {
                    case 43:
                        {
                        alt14=1;
                        }
                        break;
                    case 44:
                        {
                        alt14=2;
                        }
                        break;
                    case 45:
                        {
                        alt14=3;
                        }
                        break;
                    case 46:
                        {
                        alt14=4;
                        }
                        break;
                    case 47:
                        {
                        alt14=5;
                        }
                        break;
                    case 48:
                        {
                        alt14=6;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 14, 0, input);

                        throw nvae;
                    }

                    switch (alt14) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:30: s= '<='
                            {
                            s=(Token)match(input,43,FOLLOW_43_in_comparisonExpression574); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_43.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:39: s= '>='
                            {
                            s=(Token)match(input,44,FOLLOW_44_in_comparisonExpression580); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_44.add(s);


                            }
                            break;
                        case 3 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:48: s= '<'
                            {
                            s=(Token)match(input,45,FOLLOW_45_in_comparisonExpression586); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_45.add(s);


                            }
                            break;
                        case 4 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:56: s= '>'
                            {
                            s=(Token)match(input,46,FOLLOW_46_in_comparisonExpression592); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_46.add(s);


                            }
                            break;
                        case 5 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:64: s= '=='
                            {
                            s=(Token)match(input,47,FOLLOW_47_in_comparisonExpression598); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_47.add(s);


                            }
                            break;
                        case 6 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:145:73: s= '!='
                            {
                            s=(Token)match(input,48,FOLLOW_48_in_comparisonExpression604); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_48.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression609);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e2, e1, e2, e1, e1, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 146:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 147:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:147:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 148:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:148:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 149:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:149:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.valueOfSymbol((s!=null?s.getText():null)));
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 13, comparisonExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"

    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmeticExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:151:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final MeteorParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        MeteorParser.arithmeticExpression_return retval = new MeteorParser.arithmeticExpression_return();
        retval.start = input.LT(1);
        int arithmeticExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.multiplicationExpression_return e1 = null;

        MeteorParser.multiplicationExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression689);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( ((LA17_0>=49 && LA17_0<=50)) ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:33: (s= '+' | s= '-' )
                    int alt16=2;
                    int LA16_0 = input.LA(1);

                    if ( (LA16_0==49) ) {
                        alt16=1;
                    }
                    else if ( (LA16_0==50) ) {
                        alt16=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 16, 0, input);

                        throw nvae;
                    }
                    switch (alt16) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:34: s= '+'
                            {
                            s=(Token)match(input,49,FOLLOW_49_in_arithmeticExpression695); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:152:42: s= '-'
                            {
                            s=(Token)match(input,50,FOLLOW_50_in_arithmeticExpression701); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_50.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression706);
                    e2=multiplicationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiplicationExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 153:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 155:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 14, arithmeticExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"

    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplicationExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:157:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final MeteorParser.multiplicationExpression_return multiplicationExpression() throws RecognitionException {
        MeteorParser.multiplicationExpression_return retval = new MeteorParser.multiplicationExpression_return();
        retval.start = input.LT(1);
        int multiplicationExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.preincrementExpression_return e1 = null;

        MeteorParser.preincrementExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression749);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==STAR||LA19_0==51) ) {
                alt19=1;
            }
            switch (alt19) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:31: (s= '*' | s= '/' )
                    int alt18=2;
                    int LA18_0 = input.LA(1);

                    if ( (LA18_0==STAR) ) {
                        alt18=1;
                    }
                    else if ( (LA18_0==51) ) {
                        alt18=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 18, 0, input);

                        throw nvae;
                    }
                    switch (alt18) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression755); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:40: s= '/'
                            {
                            s=(Token)match(input,51,FOLLOW_51_in_multiplicationExpression761); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_51.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression766);
                    e2=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_preincrementExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 159:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:159:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 161:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 15, multiplicationExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"

    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "preincrementExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:163:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final MeteorParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        MeteorParser.preincrementExpression_return retval = new MeteorParser.preincrementExpression_return();
        retval.start = input.LT(1);
        int preincrementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal31=null;
        Token string_literal33=null;
        MeteorParser.preincrementExpression_return preincrementExpression32 = null;

        MeteorParser.preincrementExpression_return preincrementExpression34 = null;

        MeteorParser.unaryExpression_return unaryExpression35 = null;


        EvaluationExpression string_literal31_tree=null;
        EvaluationExpression string_literal33_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:164:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt20=3;
            switch ( input.LA(1) ) {
            case 52:
                {
                alt20=1;
                }
                break;
            case 53:
                {
                alt20=2;
                }
                break;
            case ID:
            case VAR:
            case STRING:
            case DECIMAL:
            case INTEGER:
            case 30:
            case 54:
            case 55:
            case 58:
            case 60:
            case 61:
            case 62:
            case 63:
                {
                alt20=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 20, 0, input);

                throw nvae;
            }

            switch (alt20) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:164:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal31=(Token)match(input,52,FOLLOW_52_in_preincrementExpression807); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal31_tree = (EvaluationExpression)adaptor.create(string_literal31);
                    adaptor.addChild(root_0, string_literal31_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression809);
                    preincrementExpression32=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression32.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:165:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal33=(Token)match(input,53,FOLLOW_53_in_preincrementExpression814); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal33_tree = (EvaluationExpression)adaptor.create(string_literal33);
                    adaptor.addChild(root_0, string_literal33_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression816);
                    preincrementExpression34=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression34.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:166:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression821);
                    unaryExpression35=unaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression35.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 16, preincrementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"

    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:168:1: unaryExpression : ( '!' | '~' )? castExpression ;
    public final MeteorParser.unaryExpression_return unaryExpression() throws RecognitionException {
        MeteorParser.unaryExpression_return retval = new MeteorParser.unaryExpression_return();
        retval.start = input.LT(1);
        int unaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token set36=null;
        MeteorParser.castExpression_return castExpression37 = null;


        EvaluationExpression set36_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:169:2: ( ( '!' | '~' )? castExpression )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:169:4: ( '!' | '~' )? castExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:169:4: ( '!' | '~' )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( ((LA21_0>=54 && LA21_0<=55)) ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    {
                    set36=(Token)input.LT(1);
                    if ( (input.LA(1)>=54 && input.LA(1)<=55) ) {
                        input.consume();
                        if ( state.backtracking==0 ) adaptor.addChild(root_0, (EvaluationExpression)adaptor.create(set36));
                        state.errorRecovery=false;state.failed=false;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }


                    }
                    break;

            }

            pushFollow(FOLLOW_castExpression_in_unaryExpression840);
            castExpression37=castExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, castExpression37.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 17, unaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "unaryExpression"

    public static class castExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "castExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:171:1: castExpression : ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr;
    public final MeteorParser.castExpression_return castExpression() throws RecognitionException {
        MeteorParser.castExpression_return retval = new MeteorParser.castExpression_return();
        retval.start = input.LT(1);
        int castExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token type=null;
        Token char_literal38=null;
        Token char_literal39=null;
        Token string_literal40=null;
        MeteorParser.generalPathExpression_return expr = null;


        EvaluationExpression type_tree=null;
        EvaluationExpression char_literal38_tree=null;
        EvaluationExpression char_literal39_tree=null;
        EvaluationExpression string_literal40_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:2: ( ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            {
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            int alt22=3;
            alt22 = dfa22.predict(input);
            switch (alt22) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:5: '(' type= ID ')' expr= generalPathExpression
                    {
                    char_literal38=(Token)match(input,30,FOLLOW_30_in_castExpression850); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal38);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression854); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);

                    char_literal39=(Token)match(input,32,FOLLOW_32_in_castExpression856); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal39);

                    pushFollow(FOLLOW_generalPathExpression_in_castExpression860);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:173:4: expr= generalPathExpression 'as' type= ID
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression867);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());
                    string_literal40=(Token)match(input,56,FOLLOW_56_in_castExpression869); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(string_literal40);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression873); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);


                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:174:4: expr= generalPathExpression
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression880);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: expr
            // token labels: 
            // rule labels: retval, expr
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr",expr!=null?expr.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 175:2: -> { type != null }?
            if ( type != null ) {
                adaptor.addChild(root_0,  coerce((type!=null?type.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)) );

            }
            else // 176:2: -> $expr
            {
                adaptor.addChild(root_0, stream_expr.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 18, castExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "castExpression"

    public static class generalPathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "generalPathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:1: generalPathExpression : (value= valueExpression path= pathExpression -> | valueExpression );
    public final MeteorParser.generalPathExpression_return generalPathExpression() throws RecognitionException {
        MeteorParser.generalPathExpression_return retval = new MeteorParser.generalPathExpression_return();
        retval.start = input.LT(1);
        int generalPathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.valueExpression_return value = null;

        MeteorParser.pathExpression_return path = null;

        MeteorParser.valueExpression_return valueExpression41 = null;


        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:2: (value= valueExpression path= pathExpression -> | valueExpression )
            int alt23=2;
            alt23 = dfa23.predict(input);
            switch (alt23) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:4: value= valueExpression path= pathExpression
                    {
                    pushFollow(FOLLOW_valueExpression_in_generalPathExpression907);
                    value=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(value.getTree());
                    pushFollow(FOLLOW_pathExpression_in_generalPathExpression911);
                    path=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 179:46: ->
                    {
                        adaptor.addChild(root_0,  PathExpression.wrapIfNecessary((value!=null?((EvaluationExpression)value.tree):null), (path!=null?((EvaluationExpression)path.tree):null)) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:180:4: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_generalPathExpression921);
                    valueExpression41=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, valueExpression41.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 19, generalPathExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "generalPathExpression"

    public static class contextAwarePathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwarePathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:182:1: contextAwarePathExpression[EvaluationExpression context] : path= pathExpression ->;
    public final MeteorParser.contextAwarePathExpression_return contextAwarePathExpression(EvaluationExpression context) throws RecognitionException {
        MeteorParser.contextAwarePathExpression_return retval = new MeteorParser.contextAwarePathExpression_return();
        retval.start = input.LT(1);
        int contextAwarePathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.pathExpression_return path = null;


        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:183:3: (path= pathExpression ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:183:5: path= pathExpression
            {
            pushFollow(FOLLOW_pathExpression_in_contextAwarePathExpression934);
            path=pathExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 183:25: ->
            {
                adaptor.addChild(root_0,  PathExpression.wrapIfNecessary(context, (path!=null?((EvaluationExpression)path.tree):null)) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 20, contextAwarePathExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "contextAwarePathExpression"

    protected static class pathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack pathExpression_stack = new Stack();

    public static class pathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "pathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:1: pathExpression : ( ( '.' (field= ID ) ) | arrayAccess )+ ->;
    public final MeteorParser.pathExpression_return pathExpression() throws RecognitionException {
        pathExpression_stack.push(new pathExpression_scope());
        MeteorParser.pathExpression_return retval = new MeteorParser.pathExpression_return();
        retval.start = input.LT(1);
        int pathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token field=null;
        Token char_literal42=null;
        MeteorParser.arrayAccess_return arrayAccess43 = null;


        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal42_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:188:3: ( ( ( '.' (field= ID ) ) | arrayAccess )+ ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:5: ( ( '.' (field= ID ) ) | arrayAccess )+
            {
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:5: ( ( '.' (field= ID ) ) | arrayAccess )+
            int cnt24=0;
            loop24:
            do {
                int alt24=3;
                int LA24_0 = input.LA(1);

                if ( (LA24_0==57) ) {
                    int LA24_2 = input.LA(2);

                    if ( (synpred35_Meteor()) ) {
                        alt24=1;
                    }


                }
                else if ( (LA24_0==63) ) {
                    int LA24_3 = input.LA(2);

                    if ( (synpred36_Meteor()) ) {
                        alt24=2;
                    }


                }


                switch (alt24) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:7: ( '.' (field= ID ) )
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:7: ( '.' (field= ID ) )
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:8: '.' (field= ID )
            	    {
            	    char_literal42=(Token)match(input,57,FOLLOW_57_in_pathExpression967); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_57.add(char_literal42);

            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:12: (field= ID )
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_pathExpression972); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:190:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_pathExpression990);
            	    arrayAccess43=arrayAccess();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess43.getTree());
            	    if ( state.backtracking==0 ) {
            	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add((arrayAccess43!=null?((EvaluationExpression)arrayAccess43.tree):null)); 
            	    }

            	    }
            	    break;

            	default :
            	    if ( cnt24 >= 1 ) break loop24;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(24, input);
                        throw eee;
                }
                cnt24++;
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 191:3: ->
            {
                adaptor.addChild(root_0,  PathExpression.wrapIfNecessary(((pathExpression_scope)pathExpression_stack.peek()).fragments) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 21, pathExpression_StartIndex); }
            pathExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "pathExpression"

    public static class valueExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "valueExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:193:1: valueExpression : ( methodCall[null] | parenthesesExpression | literal | VAR -> | ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation );
    public final MeteorParser.valueExpression_return valueExpression() throws RecognitionException {
        MeteorParser.valueExpression_return retval = new MeteorParser.valueExpression_return();
        retval.start = input.LT(1);
        int valueExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR47=null;
        Token ID48=null;
        MeteorParser.methodCall_return methodCall44 = null;

        MeteorParser.parenthesesExpression_return parenthesesExpression45 = null;

        MeteorParser.literal_return literal46 = null;

        MeteorParser.streamIndexAccess_return streamIndexAccess49 = null;

        MeteorParser.arrayCreation_return arrayCreation50 = null;

        MeteorParser.objectCreation_return objectCreation51 = null;


        EvaluationExpression VAR47_tree=null;
        EvaluationExpression ID48_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:194:2: ( methodCall[null] | parenthesesExpression | literal | VAR -> | ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation )
            int alt25=8;
            alt25 = dfa25.predict(input);
            switch (alt25) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:194:4: methodCall[null]
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_methodCall_in_valueExpression1011);
                    methodCall44=methodCall(null);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, methodCall44.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:195:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression1017);
                    parenthesesExpression45=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression45.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:196:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression1023);
                    literal46=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal46.getTree());

                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:197:4: VAR
                    {
                    VAR47=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression1029); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR47);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 197:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR47) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:198:5: ID {...}? =>
                    {
                    ID48=(Token)match(input,ID,FOLLOW_ID_in_valueExpression1039); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID48);

                    if ( !(( hasBinding(ID48, EvaluationExpression.class) )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "valueExpression", " hasBinding($ID, EvaluationExpression.class) ");
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 198:59: ->
                    {
                        adaptor.addChild(root_0,  getBinding(ID48, EvaluationExpression.class) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:199:5: streamIndexAccess
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_streamIndexAccess_in_valueExpression1052);
                    streamIndexAccess49=streamIndexAccess();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, streamIndexAccess49.getTree());

                    }
                    break;
                case 7 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:200:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression1057);
                    arrayCreation50=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation50.getTree());

                    }
                    break;
                case 8 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:201:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression1063);
                    objectCreation51=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation51.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 22, valueExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "valueExpression"

    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:203:1: operatorExpression : op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) ;
    public final MeteorParser.operatorExpression_return operatorExpression() throws RecognitionException {
        MeteorParser.operatorExpression_return retval = new MeteorParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        MeteorParser.operator_return op = null;


        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:204:2: (op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:204:4: op= operator
            {
            pushFollow(FOLLOW_operator_in_operatorExpression1076);
            op=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(op.getTree());


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 204:16: -> ^( EXPRESSION[\"NestedOperatorExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:204:19: ^( EXPRESSION[\"NestedOperatorExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "NestedOperatorExpression"), root_1);

                adaptor.addChild(root_1,  (op!=null?op.op:null) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 23, operatorExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"

    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "parenthesesExpression"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:206:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final MeteorParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        MeteorParser.parenthesesExpression_return retval = new MeteorParser.parenthesesExpression_return();
        retval.start = input.LT(1);
        int parenthesesExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal52=null;
        Token char_literal54=null;
        MeteorParser.expression_return expression53 = null;


        EvaluationExpression char_literal52_tree=null;
        EvaluationExpression char_literal54_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:207:2: ( ( '(' expression ')' ) -> expression )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:207:4: ( '(' expression ')' )
            {
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:207:4: ( '(' expression ')' )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:207:5: '(' expression ')'
            {
            char_literal52=(Token)match(input,30,FOLLOW_30_in_parenthesesExpression1097); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal52);

            pushFollow(FOLLOW_expression_in_parenthesesExpression1099);
            expression53=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression53.getTree());
            char_literal54=(Token)match(input,32,FOLLOW_32_in_parenthesesExpression1101); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal54);


            }



            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 207:25: -> expression
            {
                adaptor.addChild(root_0, stream_expression.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 24, parenthesesExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"

    public static class methodCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "methodCall"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:209:1: methodCall[EvaluationExpression targetExpr] : name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->;
    public final MeteorParser.methodCall_return methodCall(EvaluationExpression targetExpr) throws RecognitionException {
        MeteorParser.methodCall_return retval = new MeteorParser.methodCall_return();
        retval.start = input.LT(1);
        int methodCall_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal55=null;
        Token char_literal56=null;
        Token char_literal57=null;
        MeteorParser.expression_return param = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal55_tree=null;
        EvaluationExpression char_literal56_tree=null;
        EvaluationExpression char_literal57_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:211:2: (name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:211:4: name= ID '(' (param= expression ( ',' param= expression )* )? ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_methodCall1124); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal55=(Token)match(input,30,FOLLOW_30_in_methodCall1126); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal55);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:212:2: (param= expression ( ',' param= expression )* )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( ((LA27_0>=ID && LA27_0<=STRING)||(LA27_0>=DECIMAL && LA27_0<=INTEGER)||LA27_0==30||(LA27_0>=52 && LA27_0<=55)||LA27_0==58||(LA27_0>=60 && LA27_0<=63)||LA27_0==65||LA27_0==67) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:212:3: param= expression ( ',' param= expression )*
                    {
                    pushFollow(FOLLOW_expression_in_methodCall1133);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    if ( state.backtracking==0 ) {
                       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    }
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:2: ( ',' param= expression )*
                    loop26:
                    do {
                        int alt26=2;
                        int LA26_0 = input.LA(1);

                        if ( (LA26_0==31) ) {
                            alt26=1;
                        }


                        switch (alt26) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:213:3: ',' param= expression
                    	    {
                    	    char_literal56=(Token)match(input,31,FOLLOW_31_in_methodCall1139); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal56);

                    	    pushFollow(FOLLOW_expression_in_methodCall1143);
                    	    param=expression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    	    if ( state.backtracking==0 ) {
                    	       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop26;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal57=(Token)match(input,32,FOLLOW_32_in_methodCall1153); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal57);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 214:6: ->
            {
                adaptor.addChild(root_0,  createCheckedMethodCall(name, targetExpr, params.toArray(new EvaluationExpression[params.size()])) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 25, methodCall_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "methodCall"

    public static class fieldAssignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fieldAssignment"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:1: fieldAssignment : ( ID ':' expression -> | VAR ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) ) );
    public final MeteorParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        MeteorParser.fieldAssignment_return retval = new MeteorParser.fieldAssignment_return();
        retval.start = input.LT(1);
        int fieldAssignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token ID58=null;
        Token char_literal59=null;
        Token VAR61=null;
        Token char_literal62=null;
        Token STAR63=null;
        Token char_literal64=null;
        Token char_literal65=null;
        MeteorParser.operator_return op = null;

        MeteorParser.contextAwarePathExpression_return p = null;

        MeteorParser.expression_return e2 = null;

        MeteorParser.expression_return expression60 = null;


        EvaluationExpression ID58_tree=null;
        EvaluationExpression char_literal59_tree=null;
        EvaluationExpression VAR61_tree=null;
        EvaluationExpression char_literal62_tree=null;
        EvaluationExpression STAR63_tree=null;
        EvaluationExpression char_literal64_tree=null;
        EvaluationExpression char_literal65_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_contextAwarePathExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwarePathExpression");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:217:2: ( ID ':' expression -> | VAR ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) ) )
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==ID) ) {
                alt30=1;
            }
            else if ( (LA30_0==VAR) ) {
                alt30=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 30, 0, input);

                throw nvae;
            }
            switch (alt30) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:217:4: ID ':' expression
                    {
                    ID58=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1167); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID58);

                    char_literal59=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1169); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal59);

                    pushFollow(FOLLOW_expression_in_fieldAssignment1171);
                    expression60=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression60.getTree());
                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((ID58!=null?ID58.getText():null), (expression60!=null?((EvaluationExpression)expression60.tree):null))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 218:104: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:219:5: VAR ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) )
                    {
                    VAR61=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1186); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR61);

                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:220:5: ( '.' STAR -> | '=' op= operator {...}? => | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class).toInputSelection($operator::result)] ( ':' e2= expression -> | ->) )
                    int alt29=3;
                    switch ( input.LA(1) ) {
                    case 57:
                        {
                        int LA29_1 = input.LA(2);

                        if ( (LA29_1==STAR) ) {
                            alt29=1;
                        }
                        else if ( (LA29_1==ID) ) {
                            alt29=3;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 29, 1, input);

                            throw nvae;
                        }
                        }
                        break;
                    case 28:
                        {
                        alt29=2;
                        }
                        break;
                    case 63:
                        {
                        alt29=3;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 29, 0, input);

                        throw nvae;
                    }

                    switch (alt29) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:220:7: '.' STAR
                            {
                            char_literal62=(Token)match(input,57,FOLLOW_57_in_fieldAssignment1195); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_57.add(char_literal62);

                            STAR63=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment1197); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(STAR63);

                            if ( state.backtracking==0 ) {
                               ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(makePath(VAR61))); 
                            }


                            // AST REWRITE
                            // elements: 
                            // token labels: 
                            // rule labels: retval
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {
                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 220:98: ->
                            {
                                root_0 = null;
                            }

                            retval.tree = root_0;}
                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:221:9: '=' op= operator {...}? =>
                            {
                            char_literal64=(Token)match(input,28,FOLLOW_28_in_fieldAssignment1211); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_28.add(char_literal64);

                            pushFollow(FOLLOW_operator_in_fieldAssignment1215);
                            op=operator();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_operator.add(op.getTree());
                            if ( !(( setInnerOutput(VAR61, (op!=null?op.op:null)) )) ) {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                throw new FailedPredicateException(input, "fieldAssignment", " setInnerOutput($VAR, $op.op) ");
                            }

                            }
                            break;
                        case 3 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:222:9: p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class).toInputSelection($operator::result)] ( ':' e2= expression -> | ->)
                            {
                            pushFollow(FOLLOW_contextAwarePathExpression_in_fieldAssignment1230);
                            p=contextAwarePathExpression(getBinding(VAR61, JsonStreamExpression.class).toInputSelection(((operator_scope)operator_stack.peek()).result));

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_contextAwarePathExpression.add(p.getTree());
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:223:7: ( ':' e2= expression -> | ->)
                            int alt28=2;
                            int LA28_0 = input.LA(1);

                            if ( (LA28_0==35) ) {
                                alt28=1;
                            }
                            else if ( (LA28_0==EOF||LA28_0==31||LA28_0==59) ) {
                                alt28=2;
                            }
                            else {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                NoViableAltException nvae =
                                    new NoViableAltException("", 28, 0, input);

                                throw nvae;
                            }
                            switch (alt28) {
                                case 1 :
                                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:223:9: ':' e2= expression
                                    {
                                    char_literal65=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1241); if (state.failed) return retval; 
                                    if ( state.backtracking==0 ) stream_35.add(char_literal65);

                                    pushFollow(FOLLOW_expression_in_fieldAssignment1245);
                                    e2=expression();

                                    state._fsp--;
                                    if (state.failed) return retval;
                                    if ( state.backtracking==0 ) stream_expression.add(e2.getTree());
                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping((p!=null?((EvaluationExpression)p.tree):null), (e2!=null?((EvaluationExpression)e2.tree):null))); 
                                    }


                                    // AST REWRITE
                                    // elements: 
                                    // token labels: 
                                    // rule labels: retval
                                    // token list labels: 
                                    // rule list labels: 
                                    // wildcard labels: 
                                    if ( state.backtracking==0 ) {
                                    retval.tree = root_0;
                                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                                    root_0 = (EvaluationExpression)adaptor.nil();
                                    // 223:112: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;
                                case 2 :
                                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:224:23: 
                                    {
                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment(getAssignmentName((p!=null?((EvaluationExpression)p.tree):null)), (p!=null?((EvaluationExpression)p.tree):null))); 
                                    }


                                    // AST REWRITE
                                    // elements: 
                                    // token labels: 
                                    // rule labels: retval
                                    // token list labels: 
                                    // rule list labels: 
                                    // wildcard labels: 
                                    if ( state.backtracking==0 ) {
                                    retval.tree = root_0;
                                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                                    root_0 = (EvaluationExpression)adaptor.nil();
                                    // 224:131: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;

                            }


                            }
                            break;

                    }


                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 26, fieldAssignment_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "fieldAssignment"

    protected static class objectCreation_scope {
        List<ObjectCreation.Mapping> mappings;
    }
    protected Stack objectCreation_stack = new Stack();

    public static class objectCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "objectCreation"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final MeteorParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        MeteorParser.objectCreation_return retval = new MeteorParser.objectCreation_return();
        retval.start = input.LT(1);
        int objectCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal66=null;
        Token char_literal68=null;
        Token char_literal70=null;
        Token char_literal71=null;
        MeteorParser.fieldAssignment_return fieldAssignment67 = null;

        MeteorParser.fieldAssignment_return fieldAssignment69 = null;


        EvaluationExpression char_literal66_tree=null;
        EvaluationExpression char_literal68_tree=null;
        EvaluationExpression char_literal70_tree=null;
        EvaluationExpression char_literal71_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal66=(Token)match(input,58,FOLLOW_58_in_objectCreation1297); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(char_literal66);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( ((LA33_0>=ID && LA33_0<=VAR)) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1300);
                    fieldAssignment67=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment67.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:25: ( ',' fieldAssignment )*
                    loop31:
                    do {
                        int alt31=2;
                        int LA31_0 = input.LA(1);

                        if ( (LA31_0==31) ) {
                            int LA31_1 = input.LA(2);

                            if ( ((LA31_1>=ID && LA31_1<=VAR)) ) {
                                alt31=1;
                            }


                        }


                        switch (alt31) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:26: ',' fieldAssignment
                    	    {
                    	    char_literal68=(Token)match(input,31,FOLLOW_31_in_objectCreation1303); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal68);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1305);
                    	    fieldAssignment69=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment69.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop31;
                        }
                    } while (true);

                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:48: ( ',' )?
                    int alt32=2;
                    int LA32_0 = input.LA(1);

                    if ( (LA32_0==31) ) {
                        alt32=1;
                    }
                    switch (alt32) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ','
                            {
                            char_literal70=(Token)match(input,31,FOLLOW_31_in_objectCreation1309); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_31.add(char_literal70);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal71=(Token)match(input,59,FOLLOW_59_in_objectCreation1314); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(char_literal71);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 231:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:62: ^( EXPRESSION[\"ObjectCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ObjectCreation"), root_1);

                adaptor.addChild(root_1,  ((objectCreation_scope)objectCreation_stack.peek()).mappings );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 27, objectCreation_StartIndex); }
            objectCreation_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "objectCreation"

    public static class literal_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:233:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->);
    public final MeteorParser.literal_return literal() throws RecognitionException {
        MeteorParser.literal_return retval = new MeteorParser.literal_return();
        retval.start = input.LT(1);
        int literal_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token val=null;
        Token string_literal72=null;

        EvaluationExpression val_tree=null;
        EvaluationExpression string_literal72_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_62=new RewriteRuleTokenStream(adaptor,"token 62");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:234:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->)
            int alt34=6;
            switch ( input.LA(1) ) {
            case 60:
                {
                alt34=1;
                }
                break;
            case 61:
                {
                alt34=2;
                }
                break;
            case DECIMAL:
                {
                alt34=3;
                }
                break;
            case STRING:
                {
                alt34=4;
                }
                break;
            case INTEGER:
                {
                alt34=5;
                }
                break;
            case 62:
                {
                alt34=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;
            }

            switch (alt34) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:234:4: val= 'true'
                    {
                    val=(Token)match(input,60,FOLLOW_60_in_literal1334); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 234:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:234:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  Boolean.TRUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:235:4: val= 'false'
                    {
                    val=(Token)match(input,61,FOLLOW_61_in_literal1350); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_61.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 235:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:235:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  Boolean.FALSE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:236:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal1366); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DECIMAL.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 236:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:236:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  new BigDecimal((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal1382); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 237:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:237:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  val.getText() );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:238:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal1399); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_INTEGER.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 238:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:238:20: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  parseInt((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:239:5: 'null'
                    {
                    string_literal72=(Token)match(input,62,FOLLOW_62_in_literal1414); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_62.add(string_literal72);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 239:12: ->
                    {
                        adaptor.addChild(root_0,  ConstantExpression.NULL );

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 28, literal_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:241:1: arrayAccess : ( '[' STAR ']' path= pathExpression -> ^( EXPRESSION[\"ArrayProjection\"] $path) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final MeteorParser.arrayAccess_return arrayAccess() throws RecognitionException {
        MeteorParser.arrayAccess_return retval = new MeteorParser.arrayAccess_return();
        retval.start = input.LT(1);
        int arrayAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal73=null;
        Token STAR74=null;
        Token char_literal75=null;
        Token char_literal76=null;
        Token char_literal77=null;
        Token char_literal78=null;
        Token char_literal79=null;
        Token char_literal80=null;
        MeteorParser.pathExpression_return path = null;


        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal73_tree=null;
        EvaluationExpression STAR74_tree=null;
        EvaluationExpression char_literal75_tree=null;
        EvaluationExpression char_literal76_tree=null;
        EvaluationExpression char_literal77_tree=null;
        EvaluationExpression char_literal78_tree=null;
        EvaluationExpression char_literal79_tree=null;
        EvaluationExpression char_literal80_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 29) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:242:3: ( '[' STAR ']' path= pathExpression -> ^( EXPRESSION[\"ArrayProjection\"] $path) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt38=3;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==63) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt38=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA38_3 = input.LA(3);

                    if ( (LA38_3==64) ) {
                        alt38=2;
                    }
                    else if ( (LA38_3==35) ) {
                        alt38=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 38, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA38_4 = input.LA(3);

                    if ( (LA38_4==64) ) {
                        alt38=2;
                    }
                    else if ( (LA38_4==35) ) {
                        alt38=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 38, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 38, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 38, 0, input);

                throw nvae;
            }
            switch (alt38) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:242:5: '[' STAR ']' path= pathExpression
                    {
                    char_literal73=(Token)match(input,63,FOLLOW_63_in_arrayAccess1428); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal73);

                    STAR74=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1430); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR74);

                    char_literal75=(Token)match(input,64,FOLLOW_64_in_arrayAccess1432); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal75);

                    pushFollow(FOLLOW_pathExpression_in_arrayAccess1436);
                    path=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


                    // AST REWRITE
                    // elements: path
                    // token labels: 
                    // rule labels: retval, path
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 243:3: -> ^( EXPRESSION[\"ArrayProjection\"] $path)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:243:6: ^( EXPRESSION[\"ArrayProjection\"] $path)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayProjection"), root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:5: '[' (pos= INTEGER | pos= UINT ) ']'
                    {
                    char_literal76=(Token)match(input,63,FOLLOW_63_in_arrayAccess1456); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal76);

                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:9: (pos= INTEGER | pos= UINT )
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==INTEGER) ) {
                        alt35=1;
                    }
                    else if ( (LA35_0==UINT) ) {
                        alt35=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 35, 0, input);

                        throw nvae;
                    }
                    switch (alt35) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1461); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1467); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }

                    char_literal77=(Token)match(input,64,FOLLOW_64_in_arrayAccess1470); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal77);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 245:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:245:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((pos!=null?pos.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:5: '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']'
                    {
                    char_literal78=(Token)match(input,63,FOLLOW_63_in_arrayAccess1488); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal78);

                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:9: (start= INTEGER | start= UINT )
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==INTEGER) ) {
                        alt36=1;
                    }
                    else if ( (LA36_0==UINT) ) {
                        alt36=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 36, 0, input);

                        throw nvae;
                    }
                    switch (alt36) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1493); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1499); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }

                    char_literal79=(Token)match(input,35,FOLLOW_35_in_arrayAccess1502); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal79);

                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:42: (end= INTEGER | end= UINT )
                    int alt37=2;
                    int LA37_0 = input.LA(1);

                    if ( (LA37_0==INTEGER) ) {
                        alt37=1;
                    }
                    else if ( (LA37_0==UINT) ) {
                        alt37=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 37, 0, input);

                        throw nvae;
                    }
                    switch (alt37) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1507); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1513); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }

                    char_literal80=(Token)match(input,64,FOLLOW_64_in_arrayAccess1516); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal80);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 247:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:247:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((start!=null?start.getText():null)) );
                        adaptor.addChild(root_1,  Integer.valueOf((end!=null?end.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 29, arrayAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class streamIndexAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "streamIndexAccess"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:249:1: streamIndexAccess : op= VAR {...}? => '[' path= generalPathExpression ']' {...}? ->;
    public final MeteorParser.streamIndexAccess_return streamIndexAccess() throws RecognitionException {
        MeteorParser.streamIndexAccess_return retval = new MeteorParser.streamIndexAccess_return();
        retval.start = input.LT(1);
        int streamIndexAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token op=null;
        Token char_literal81=null;
        Token char_literal82=null;
        MeteorParser.generalPathExpression_return path = null;


        EvaluationExpression op_tree=null;
        EvaluationExpression char_literal81_tree=null;
        EvaluationExpression char_literal82_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 30) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:3: (op= VAR {...}? => '[' path= generalPathExpression ']' {...}? ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:5: op= VAR {...}? => '[' path= generalPathExpression ']' {...}?
            {
            op=(Token)match(input,VAR,FOLLOW_VAR_in_streamIndexAccess1544); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(op);

            if ( !(( getRawBinding(op, JsonStreamExpression.class) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "streamIndexAccess", " getRawBinding($op, JsonStreamExpression.class) != null ");
            }
            char_literal81=(Token)match(input,63,FOLLOW_63_in_streamIndexAccess1553); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal81);

            pushFollow(FOLLOW_generalPathExpression_in_streamIndexAccess1557);
            path=generalPathExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_generalPathExpression.add(path.getTree());
            char_literal82=(Token)match(input,64,FOLLOW_64_in_streamIndexAccess1559); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal82);

            if ( !(( !((path!=null?((EvaluationExpression)path.tree):null) instanceof ConstantExpression) )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "streamIndexAccess", " !($path.tree instanceof ConstantExpression) ");
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 252:3: ->
            {
                adaptor.addChild(root_0,  new StreamIndexExpression(getBinding(op, JsonStreamExpression.class).getStream(), (path!=null?((EvaluationExpression)path.tree):null)) );

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 30, streamIndexAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "streamIndexAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:254:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) ;
    public final MeteorParser.arrayCreation_return arrayCreation() throws RecognitionException {
        MeteorParser.arrayCreation_return retval = new MeteorParser.arrayCreation_return();
        retval.start = input.LT(1);
        int arrayCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal83=null;
        Token char_literal84=null;
        Token char_literal85=null;
        Token char_literal86=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal83_tree=null;
        EvaluationExpression char_literal84_tree=null;
        EvaluationExpression char_literal85_tree=null;
        EvaluationExpression char_literal86_tree=null;
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 31) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal83=(Token)match(input,63,FOLLOW_63_in_arrayCreation1578); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal83);

            pushFollow(FOLLOW_expression_in_arrayCreation1582);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:27: ( ',' elems+= expression )*
            loop39:
            do {
                int alt39=2;
                int LA39_0 = input.LA(1);

                if ( (LA39_0==31) ) {
                    int LA39_1 = input.LA(2);

                    if ( ((LA39_1>=ID && LA39_1<=STRING)||(LA39_1>=DECIMAL && LA39_1<=INTEGER)||LA39_1==30||(LA39_1>=52 && LA39_1<=55)||LA39_1==58||(LA39_1>=60 && LA39_1<=63)||LA39_1==65||LA39_1==67) ) {
                        alt39=1;
                    }


                }


                switch (alt39) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:28: ',' elems+= expression
            	    {
            	    char_literal84=(Token)match(input,31,FOLLOW_31_in_arrayCreation1585); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_31.add(char_literal84);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1589);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop39;
                }
            } while (true);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:52: ( ',' )?
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( (LA40_0==31) ) {
                alt40=1;
            }
            switch (alt40) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ','
                    {
                    char_literal85=(Token)match(input,31,FOLLOW_31_in_arrayCreation1593); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal85);


                    }
                    break;

            }

            char_literal86=(Token)match(input,64,FOLLOW_64_in_arrayCreation1596); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal86);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 255:61: -> ^( EXPRESSION[\"ArrayCreation\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:64: ^( EXPRESSION[\"ArrayCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayCreation"), root_1);

                adaptor.addChild(root_1,  list_elems.toArray(new EvaluationExpression[list_elems.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 31, arrayCreation_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"

    protected static class operator_scope {
        Operator<?> result;
        int numInputs;
        Map<JsonStream, List<ExpressionTag>> inputTags;
    }
    protected Stack operator_stack = new Stack();

    public static class operator_return extends ParserRuleReturnScope {
        public Operator<?> op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:257:1: operator returns [Operator<?> op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
    public final MeteorParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        MeteorParser.operator_return retval = new MeteorParser.operator_return();
        retval.start = input.LT(1);
        int operator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token opRule=null;
        MeteorParser.readOperator_return readOperator87 = null;

        MeteorParser.writeOperator_return writeOperator88 = null;

        MeteorParser.genericOperator_return genericOperator89 = null;


        EvaluationExpression opRule_tree=null;


          if(state.backtracking == 0) 
        	  getContext().getBindings().addScope();
        	((operator_scope)operator_stack.peek()).inputTags = new IdentityHashMap<JsonStream, List<ExpressionTag>>();

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 32) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:11: ( readOperator | writeOperator | genericOperator )
            int alt41=3;
            switch ( input.LA(1) ) {
            case 65:
                {
                alt41=1;
                }
                break;
            case 67:
                {
                alt41=2;
                }
                break;
            case ID:
                {
                alt41=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 41, 0, input);

                throw nvae;
            }

            switch (alt41) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1633);
                    readOperator87=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator87.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1637);
                    writeOperator88=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator88.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1641);
                    genericOperator89=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator89.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
                retval.op = ((operator_scope)operator_stack.peek()).result;

            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {

                getContext().getBindings().removeScope();

            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 32, operator_StartIndex); }
            operator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operator"

    public static class readOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "readOperator"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:1: readOperator : 'read' 'from' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final MeteorParser.readOperator_return readOperator() throws RecognitionException {
        MeteorParser.readOperator_return retval = new MeteorParser.readOperator_return();
        retval.start = input.LT(1);
        int readOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token loc=null;
        Token file=null;
        Token string_literal90=null;
        Token string_literal91=null;
        Token char_literal92=null;
        Token char_literal93=null;

        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal90_tree=null;
        EvaluationExpression string_literal91_tree=null;
        EvaluationExpression char_literal92_tree=null;
        EvaluationExpression char_literal93_tree=null;
        RewriteRuleTokenStream stream_66=new RewriteRuleTokenStream(adaptor,"token 66");
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_65=new RewriteRuleTokenStream(adaptor,"token 65");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 33) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:2: ( 'read' 'from' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:4: 'read' 'from' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal90=(Token)match(input,65,FOLLOW_65_in_readOperator1655); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_65.add(string_literal90);

            string_literal91=(Token)match(input,66,FOLLOW_66_in_readOperator1657); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_66.add(string_literal91);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:18: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==ID) ) {
                int LA43_1 = input.LA(2);

                if ( (LA43_1==30) ) {
                    alt43=2;
                }
                else if ( (LA43_1==STRING) ) {
                    alt43=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 43, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA43_0==STRING) ) {
                alt43=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 43, 0, input);

                throw nvae;
            }
            switch (alt43) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:19: (loc= ID )? file= STRING
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:22: (loc= ID )?
                    int alt42=2;
                    int LA42_0 = input.LA(1);

                    if ( (LA42_0==ID) ) {
                        alt42=1;
                    }
                    switch (alt42) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1662); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1667); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:276:41: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1673); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal92=(Token)match(input,30,FOLLOW_30_in_readOperator1675); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal92);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1679); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal93=(Token)match(input,32,FOLLOW_32_in_readOperator1681); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal93);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               ((operator_scope)operator_stack.peek()).result = new Source(JsonInputFormat.class, (file!=null?file.getText():null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 276:140: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 33, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:278:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final MeteorParser.writeOperator_return writeOperator() throws RecognitionException {
        MeteorParser.writeOperator_return retval = new MeteorParser.writeOperator_return();
        retval.start = input.LT(1);
        int writeOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token loc=null;
        Token file=null;
        Token string_literal94=null;
        Token string_literal95=null;
        Token char_literal96=null;
        Token char_literal97=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal94_tree=null;
        EvaluationExpression string_literal95_tree=null;
        EvaluationExpression char_literal96_tree=null;
        EvaluationExpression char_literal97_tree=null;
        RewriteRuleTokenStream stream_67=new RewriteRuleTokenStream(adaptor,"token 67");
        RewriteRuleTokenStream stream_68=new RewriteRuleTokenStream(adaptor,"token 68");
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 34) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal94=(Token)match(input,67,FOLLOW_67_in_writeOperator1695); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_67.add(string_literal94);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1699); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal95=(Token)match(input,68,FOLLOW_68_in_writeOperator1701); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_68.add(string_literal95);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==ID) ) {
                int LA45_1 = input.LA(2);

                if ( (LA45_1==30) ) {
                    alt45=2;
                }
                else if ( (LA45_1==STRING) ) {
                    alt45=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA45_0==STRING) ) {
                alt45=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 45, 0, input);

                throw nvae;
            }
            switch (alt45) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:27: (loc= ID )? file= STRING
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:30: (loc= ID )?
                    int alt44=2;
                    int LA44_0 = input.LA(1);

                    if ( (LA44_0==ID) ) {
                        alt44=1;
                    }
                    switch (alt44) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1706); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1711); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1717); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal96=(Token)match(input,30,FOLLOW_30_in_writeOperator1719); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal96);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1723); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal97=(Token)match(input,32,FOLLOW_32_in_writeOperator1725); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal97);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
              	Sink sink = new Sink(JsonOutputFormat.class, (file!=null?file.getText():null));
                ((operator_scope)operator_stack.peek()).result = sink;
                sink.setInputs(getBinding(from, JsonStreamExpression.class).getStream());
                this.sinks.add(sink);

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 285:3: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 34, writeOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "writeOperator"

    protected static class genericOperator_scope {
        OperatorInfo<?> operatorInfo;
    }
    protected Stack genericOperator_stack = new Stack();

    public static class genericOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "genericOperator"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:287:1: genericOperator : name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )* ->;
    public final MeteorParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        MeteorParser.genericOperator_return retval = new MeteorParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal101=null;
        MeteorParser.operatorFlag_return operatorFlag98 = null;

        MeteorParser.arrayInput_return arrayInput99 = null;

        MeteorParser.input_return input100 = null;

        MeteorParser.input_return input102 = null;

        MeteorParser.operatorOption_return operatorOption103 = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal101_tree=null;
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_operatorFlag=new RewriteRuleSubtreeStream(adaptor,"rule operatorFlag");
        RewriteRuleSubtreeStream stream_arrayInput=new RewriteRuleSubtreeStream(adaptor,"rule arrayInput");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 35) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:290:3: (name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )* ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:290:5: name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )*
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1745); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = findOperatorGreedily(name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "genericOperator", " ($genericOperator::operatorInfo = findOperatorGreedily($name)) != null ");
            }
            if ( state.backtracking==0 ) {
               ((operator_scope)operator_stack.peek()).result = ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.newInstance(); 
            }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:1: ( operatorFlag )*
            loop46:
            do {
                int alt46=2;
                int LA46_0 = input.LA(1);

                if ( (LA46_0==ID) ) {
                    alt46=1;
                }


                switch (alt46) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: operatorFlag
            	    {
            	    pushFollow(FOLLOW_operatorFlag_in_genericOperator1753);
            	    operatorFlag98=operatorFlag();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorFlag.add(operatorFlag98.getTree());

            	    }
            	    break;

            	default :
            	    break loop46;
                }
            } while (true);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:1: ( arrayInput | input ( ',' input )* )
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==63) ) {
                alt48=1;
            }
            else if ( (LA48_0==VAR||LA48_0==69) ) {
                alt48=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 48, 0, input);

                throw nvae;
            }
            switch (alt48) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:2: arrayInput
                    {
                    pushFollow(FOLLOW_arrayInput_in_genericOperator1757);
                    arrayInput99=arrayInput();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arrayInput.add(arrayInput99.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:15: input ( ',' input )*
                    {
                    pushFollow(FOLLOW_input_in_genericOperator1761);
                    input100=input();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_input.add(input100.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:21: ( ',' input )*
                    loop47:
                    do {
                        int alt47=2;
                        int LA47_0 = input.LA(1);

                        if ( (LA47_0==31) ) {
                            int LA47_2 = input.LA(2);

                            if ( (synpred73_Meteor()) ) {
                                alt47=1;
                            }


                        }


                        switch (alt47) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:22: ',' input
                    	    {
                    	    char_literal101=(Token)match(input,31,FOLLOW_31_in_genericOperator1764); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal101);

                    	    pushFollow(FOLLOW_input_in_genericOperator1766);
                    	    input102=input();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_input.add(input102.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop47;
                        }
                    } while (true);


                    }
                    break;

            }

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:1: ( operatorOption )*
            loop49:
            do {
                int alt49=2;
                int LA49_0 = input.LA(1);

                if ( (LA49_0==ID) ) {
                    int LA49_2 = input.LA(2);

                    if ( (synpred74_Meteor()) ) {
                        alt49=1;
                    }


                }


                switch (alt49) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: operatorOption
            	    {
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1772);
            	    operatorOption103=operatorOption();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorOption.add(operatorOption103.getTree());

            	    }
            	    break;

            	default :
            	    break loop49;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 294:17: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 35, genericOperator_StartIndex); }
            genericOperator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "genericOperator"

    protected static class operatorOption_scope {
        OperatorInfo.OperatorPropertyInfo property;
    }
    protected Stack operatorOption_stack = new Stack();

    public static class operatorOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorOption"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:296:1: operatorOption : name= ID {...}?expr= contextAwareExpression[null] ->;
    public final MeteorParser.operatorOption_return operatorOption() throws RecognitionException {
        operatorOption_stack.push(new operatorOption_scope());
        MeteorParser.operatorOption_return retval = new MeteorParser.operatorOption_return();
        retval.start = input.LT(1);
        int operatorOption_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        MeteorParser.contextAwareExpression_return expr = null;


        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 36) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:300:2: (name= ID {...}?expr= contextAwareExpression[null] ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:300:4: name= ID {...}?expr= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1792); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((operatorOption_scope)operatorOption_stack.peek()).property = findOperatorPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "operatorOption", " ($operatorOption::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null ");
            }
            pushFollow(FOLLOW_contextAwareExpression_in_operatorOption1798);
            expr=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
            if ( state.backtracking==0 ) {
               ((operatorOption_scope)operatorOption_stack.peek()).property.setValue(((operator_scope)operator_stack.peek()).result, (expr!=null?((EvaluationExpression)expr.tree):null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 301:106: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 36, operatorOption_StartIndex); }
            operatorOption_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorOption"

    protected static class operatorFlag_scope {
        OperatorInfo.OperatorPropertyInfo property;
    }
    protected Stack operatorFlag_stack = new Stack();

    public static class operatorFlag_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorFlag"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:303:1: operatorFlag : name= ID {...}? ->;
    public final MeteorParser.operatorFlag_return operatorFlag() throws RecognitionException {
        operatorFlag_stack.push(new operatorFlag_scope());
        MeteorParser.operatorFlag_return retval = new MeteorParser.operatorFlag_return();
        retval.start = input.LT(1);
        int operatorFlag_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;

        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 37) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:307:3: (name= ID {...}? ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:307:5: name= ID {...}?
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1819); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((operatorFlag_scope)operatorFlag_stack.peek()).property = findOperatorPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "operatorFlag", " ($operatorFlag::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null ");
            }
            if ( state.backtracking==0 ) {
               if(!((operatorFlag_scope)operatorFlag_stack.peek()).property.isFlag())
                  throw new QueryParserException(String.format("Property %s is not a flag", (name!=null?name.getText():null)), name);
                ((operatorFlag_scope)operatorFlag_stack.peek()).property.setValue(((operator_scope)operator_stack.peek()).result, true); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 310:64: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 37, operatorFlag_StartIndex); }
            operatorFlag_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorFlag"

    protected static class input_scope {
        OperatorInfo.InputPropertyInfo inputProperty;
    }
    protected Stack input_stack = new Stack();

    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "input"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:312:1: input : (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )? ->;
    public final MeteorParser.input_return input() throws RecognitionException {
        input_stack.push(new input_scope());
        MeteorParser.input_return retval = new MeteorParser.input_return();
        retval.start = input.LT(1);
        int input_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token preserveFlag=null;
        Token name=null;
        Token from=null;
        Token inputOption=null;
        Token string_literal104=null;
        MeteorParser.contextAwareExpression_return expr = null;


        EvaluationExpression preserveFlag_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression inputOption_tree=null;
        EvaluationExpression string_literal104_tree=null;
        RewriteRuleTokenStream stream_69=new RewriteRuleTokenStream(adaptor,"token 69");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 38) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:315:3: ( (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )? ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:315:5: (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?
            {
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:315:17: (preserveFlag= 'preserve' )?
            int alt50=2;
            int LA50_0 = input.LA(1);

            if ( (LA50_0==69) ) {
                alt50=1;
            }
            switch (alt50) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: preserveFlag= 'preserve'
                    {
                    preserveFlag=(Token)match(input,69,FOLLOW_69_in_input1841); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_69.add(preserveFlag);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
            }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:315:33: (name= VAR 'in' )?
            int alt51=2;
            int LA51_0 = input.LA(1);

            if ( (LA51_0==VAR) ) {
                int LA51_1 = input.LA(2);

                if ( (LA51_1==42) ) {
                    alt51=1;
                }
            }
            switch (alt51) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:315:34: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1849); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal104=(Token)match(input,42,FOLLOW_42_in_input1851); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal104);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1857); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                int inputIndex = ((operator_scope)operator_stack.peek()).numInputs++;
                JsonStreamExpression input = getBinding(from, JsonStreamExpression.class);
                ((operator_scope)operator_stack.peek()).result.setInput(inputIndex, input.getStream());
                
                if(preserveFlag != null)
                  setBinding(name != null ? name : from, new JsonStreamExpression(input.getStream(), inputIndex).withTag(ExpressionTag.RETAIN));
                else setBinding(name != null ? name : from, new JsonStreamExpression(input.getStream(), inputIndex));

            }
            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) {
                  addScope();
                }

            }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:329:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?
            int alt52=2;
            alt52 = dfa52.predict(input);
            switch (alt52) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:329:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)]
                    {
                    inputOption=(Token)match(input,ID,FOLLOW_ID_in_input1867); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(inputOption);

                    if ( !(( (((input_scope)input_stack.peek()).inputProperty = findInputPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, inputOption)) != null )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", " ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null ");
                    }
                    pushFollow(FOLLOW_contextAwareExpression_in_input1875);
                    expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).numInputs - 1));

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
                    if ( state.backtracking==0 ) {
                       ((input_scope)input_stack.peek()).inputProperty.setValue(((operator_scope)operator_stack.peek()).result, ((operator_scope)operator_stack.peek()).numInputs-1, (expr!=null?((EvaluationExpression)expr.tree):null)); 
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) 
                  removeScope();

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 334:1: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 38, input_StartIndex); }
            input_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "input"

    public static class arrayInput_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayInput"
    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:336:1: arrayInput : '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR ->;
    public final MeteorParser.arrayInput_return arrayInput() throws RecognitionException {
        MeteorParser.arrayInput_return retval = new MeteorParser.arrayInput_return();
        retval.start = input.LT(1);
        int arrayInput_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token char_literal105=null;
        Token char_literal106=null;
        Token char_literal107=null;
        Token string_literal108=null;
        Token names=null;
        List list_names=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression char_literal105_tree=null;
        EvaluationExpression char_literal106_tree=null;
        EvaluationExpression char_literal107_tree=null;
        EvaluationExpression string_literal108_tree=null;
        EvaluationExpression names_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 39) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:337:3: ( '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR ->)
            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:337:5: '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR
            {
            char_literal105=(Token)match(input,63,FOLLOW_63_in_arrayInput1897); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal105);

            names=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput1901); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(names);

            if (list_names==null) list_names=new ArrayList();
            list_names.add(names);

            // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:337:20: ( ',' names+= VAR )?
            int alt53=2;
            int LA53_0 = input.LA(1);

            if ( (LA53_0==31) ) {
                alt53=1;
            }
            switch (alt53) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:337:21: ',' names+= VAR
                    {
                    char_literal106=(Token)match(input,31,FOLLOW_31_in_arrayInput1904); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal106);

                    names=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput1908); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(names);

                    if (list_names==null) list_names=new ArrayList();
                    list_names.add(names);


                    }
                    break;

            }

            char_literal107=(Token)match(input,64,FOLLOW_64_in_arrayInput1912); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal107);

            string_literal108=(Token)match(input,42,FOLLOW_42_in_arrayInput1914); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(string_literal108);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput1918); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                ((operator_scope)operator_stack.peek()).result.setInput(0, getBinding(from, JsonStreamExpression.class).getStream());
                for(int index = 0; index < list_names.size(); index++) {
              	  setBinding((Token) list_names.get(index), new InputSelection(index)); 
                }

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 343:3: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 39, arrayInput_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayInput"

    // $ANTLR start synpred8_Meteor
    public final void synpred8_Meteor_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:118:5: ( ternaryExpression )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:118:5: ternaryExpression
        {
        pushFollow(FOLLOW_ternaryExpression_in_synpred8_Meteor336);
        ternaryExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred8_Meteor

    // $ANTLR start synpred10_Meteor
    public final void synpred10_Meteor_fragment() throws RecognitionException {   
        MeteorParser.orExpression_return ifClause = null;

        MeteorParser.expression_return ifExpr = null;

        MeteorParser.expression_return elseExpr = null;


        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:4: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        {
        pushFollow(FOLLOW_orExpression_in_synpred10_Meteor353);
        ifClause=orExpression();

        state._fsp--;
        if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:27: '?' (ifExpr= expression )? ':' elseExpr= expression
        {
        match(input,34,FOLLOW_34_in_synpred10_Meteor356); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:37: (ifExpr= expression )?
        int alt55=2;
        int LA55_0 = input.LA(1);

        if ( ((LA55_0>=ID && LA55_0<=STRING)||(LA55_0>=DECIMAL && LA55_0<=INTEGER)||LA55_0==30||(LA55_0>=52 && LA55_0<=55)||LA55_0==58||(LA55_0>=60 && LA55_0<=63)||LA55_0==65||LA55_0==67) ) {
            alt55=1;
        }
        switch (alt55) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:0:0: ifExpr= expression
                {
                pushFollow(FOLLOW_expression_in_synpred10_Meteor360);
                ifExpr=expression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }

        match(input,35,FOLLOW_35_in_synpred10_Meteor363); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred10_Meteor367);
        elseExpr=expression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred10_Meteor

    // $ANTLR start synpred11_Meteor
    public final void synpred11_Meteor_fragment() throws RecognitionException {   
        MeteorParser.orExpression_return ifExpr2 = null;

        MeteorParser.expression_return ifClause2 = null;


        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:124:4: (ifExpr2= orExpression 'if' ifClause2= expression )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:124:4: ifExpr2= orExpression 'if' ifClause2= expression
        {
        pushFollow(FOLLOW_orExpression_in_synpred11_Meteor390);
        ifExpr2=orExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,36,FOLLOW_36_in_synpred11_Meteor392); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred11_Meteor396);
        ifClause2=expression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred11_Meteor

    // $ANTLR start synpred32_Meteor
    public final void synpred32_Meteor_fragment() throws RecognitionException {   
        Token type=null;
        MeteorParser.generalPathExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:5: ( '(' type= ID ')' expr= generalPathExpression )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:5: '(' type= ID ')' expr= generalPathExpression
        {
        match(input,30,FOLLOW_30_in_synpred32_Meteor850); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred32_Meteor854); if (state.failed) return ;
        match(input,32,FOLLOW_32_in_synpred32_Meteor856); if (state.failed) return ;
        pushFollow(FOLLOW_generalPathExpression_in_synpred32_Meteor860);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred32_Meteor

    // $ANTLR start synpred33_Meteor
    public final void synpred33_Meteor_fragment() throws RecognitionException {   
        Token type=null;
        MeteorParser.generalPathExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:173:4: (expr= generalPathExpression 'as' type= ID )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:173:4: expr= generalPathExpression 'as' type= ID
        {
        pushFollow(FOLLOW_generalPathExpression_in_synpred33_Meteor867);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,56,FOLLOW_56_in_synpred33_Meteor869); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred33_Meteor873); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred33_Meteor

    // $ANTLR start synpred34_Meteor
    public final void synpred34_Meteor_fragment() throws RecognitionException {   
        MeteorParser.valueExpression_return value = null;

        MeteorParser.pathExpression_return path = null;


        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:4: (value= valueExpression path= pathExpression )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:179:4: value= valueExpression path= pathExpression
        {
        pushFollow(FOLLOW_valueExpression_in_synpred34_Meteor907);
        value=valueExpression();

        state._fsp--;
        if (state.failed) return ;
        pushFollow(FOLLOW_pathExpression_in_synpred34_Meteor911);
        path=pathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred34_Meteor

    // $ANTLR start synpred35_Meteor
    public final void synpred35_Meteor_fragment() throws RecognitionException {   
        Token field=null;

        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:7: ( ( '.' (field= ID ) ) )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:7: ( '.' (field= ID ) )
        {
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:7: ( '.' (field= ID ) )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:8: '.' (field= ID )
        {
        match(input,57,FOLLOW_57_in_synpred35_Meteor967); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:12: (field= ID )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:189:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred35_Meteor972); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred35_Meteor

    // $ANTLR start synpred36_Meteor
    public final void synpred36_Meteor_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:190:11: ( arrayAccess )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:190:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred36_Meteor990);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred36_Meteor

    // $ANTLR start synpred40_Meteor
    public final void synpred40_Meteor_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:197:4: ( VAR )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:197:4: VAR
        {
        match(input,VAR,FOLLOW_VAR_in_synpred40_Meteor1029); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred40_Meteor

    // $ANTLR start synpred42_Meteor
    public final void synpred42_Meteor_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:199:5: ( streamIndexAccess )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:199:5: streamIndexAccess
        {
        pushFollow(FOLLOW_streamIndexAccess_in_synpred42_Meteor1052);
        streamIndexAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred42_Meteor

    // $ANTLR start synpred73_Meteor
    public final void synpred73_Meteor_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:22: ( ',' input )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:293:22: ',' input
        {
        match(input,31,FOLLOW_31_in_synpred73_Meteor1764); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred73_Meteor1766);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred73_Meteor

    // $ANTLR start synpred74_Meteor
    public final void synpred74_Meteor_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:1: ( operatorOption )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:1: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred74_Meteor1772);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred74_Meteor

    // $ANTLR start synpred77_Meteor
    public final void synpred77_Meteor_fragment() throws RecognitionException {   
        Token inputOption=null;
        MeteorParser.contextAwareExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:329:2: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )
        // /Users/arv/Proggn/Uni/PhD/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:329:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)]
        {
        inputOption=(Token)match(input,ID,FOLLOW_ID_in_synpred77_Meteor1867); if (state.failed) return ;
        if ( !(( (((input_scope)input_stack.peek()).inputProperty = findInputPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, inputOption)) != null )) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred77_Meteor", " ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null ");
        }
        pushFollow(FOLLOW_contextAwareExpression_in_synpred77_Meteor1875);
        expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).numInputs - 1));

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred77_Meteor

    // Delegated rules

    public final boolean synpred32_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred32_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred34_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred34_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred42_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred42_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred8_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred8_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred36_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred36_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred74_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred74_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred10_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred10_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred11_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred11_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred40_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred40_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred73_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred73_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred35_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred35_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred33_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred33_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred77_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred77_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA5 dfa5 = new DFA5(this);
    protected DFA7 dfa7 = new DFA7(this);
    protected DFA22 dfa22 = new DFA22(this);
    protected DFA23 dfa23 = new DFA23(this);
    protected DFA25 dfa25 = new DFA25(this);
    protected DFA52 dfa52 = new DFA52(this);
    static final String DFA5_eotS =
        "\17\uffff";
    static final String DFA5_eofS =
        "\2\uffff\1\1\10\uffff\1\1\3\uffff";
    static final String DFA5_minS =
        "\1\6\1\uffff\1\6\1\uffff\1\7\1\6\2\0\1\6\1\11\2\6\1\11\1\6\1\0";
    static final String DFA5_maxS =
        "\1\103\1\uffff\1\105\1\uffff\1\14\1\105\2\0\1\103\1\100\1\103\2"+
        "\100\1\77\1\0";
    static final String DFA5_acceptS =
        "\1\uffff\1\1\1\uffff\1\2\13\uffff";
    static final String DFA5_specialS =
        "\6\uffff\1\0\1\2\6\uffff\1\1}>";
    static final String[] DFA5_transitionS = {
            "\1\2\2\1\1\uffff\2\1\22\uffff\1\1\25\uffff\4\1\2\uffff\1\1\1"+
            "\uffff\4\1\1\uffff\1\3\1\uffff\1\3",
            "",
            "\1\5\1\3\1\uffff\1\1\20\uffff\1\1\3\uffff\3\1\1\uffff\22\1"+
            "\4\uffff\2\1\1\uffff\1\1\3\uffff\1\4\1\1\4\uffff\1\3",
            "",
            "\1\3\1\uffff\1\1\1\uffff\2\1",
            "\1\6\1\7\1\1\1\uffff\2\1\22\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\3\1\1\10\1\uffff\1\1\1\uffff\1\1\1\uffff\1\3",
            "\1\uffff",
            "\1\uffff",
            "\1\1\1\11\1\1\1\uffff\2\1\22\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\4\1\1\uffff\1\1\1\uffff\1\1",
            "\1\1\25\uffff\1\12\2\uffff\1\1\1\uffff\20\1\4\uffff\2\1\5\uffff"+
            "\1\1\1\13",
            "\1\1\1\14\1\1\1\uffff\2\1\22\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\6\1\1\uffff\1\1",
            "\1\1\2\uffff\1\1\20\uffff\1\1\4\uffff\2\1\1\uffff\10\1\1\15"+
            "\11\1\4\uffff\2\1\1\uffff\1\1\3\uffff\2\1",
            "\1\1\25\uffff\1\1\2\uffff\1\1\1\uffff\20\1\4\uffff\2\1\5\uffff"+
            "\1\1\1\13",
            "\1\1\1\16\1\1\1\uffff\2\1\22\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\4\1",
            "\1\uffff"
    };

    static final short[] DFA5_eot = DFA.unpackEncodedString(DFA5_eotS);
    static final short[] DFA5_eof = DFA.unpackEncodedString(DFA5_eofS);
    static final char[] DFA5_min = DFA.unpackEncodedStringToUnsignedChars(DFA5_minS);
    static final char[] DFA5_max = DFA.unpackEncodedStringToUnsignedChars(DFA5_maxS);
    static final short[] DFA5_accept = DFA.unpackEncodedString(DFA5_acceptS);
    static final short[] DFA5_special = DFA.unpackEncodedString(DFA5_specialS);
    static final short[][] DFA5_transition;

    static {
        int numStates = DFA5_transitionS.length;
        DFA5_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA5_transition[i] = DFA.unpackEncodedString(DFA5_transitionS[i]);
        }
    }

    class DFA5 extends DFA {

        public DFA5(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 5;
            this.eot = DFA5_eot;
            this.eof = DFA5_eof;
            this.min = DFA5_min;
            this.max = DFA5_max;
            this.accept = DFA5_accept;
            this.special = DFA5_special;
            this.transition = DFA5_transition;
        }
        public String getDescription() {
            return "117:1: expression : ( ternaryExpression | operatorExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA5_6 = input.LA(1);

                         
                        int index5_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred8_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index5_6);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA5_14 = input.LA(1);

                         
                        int index5_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred8_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index5_14);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA5_7 = input.LA(1);

                         
                        int index5_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred8_Meteor()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index5_7);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 5, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA7_eotS =
        "\22\uffff";
    static final String DFA7_eofS =
        "\22\uffff";
    static final String DFA7_minS =
        "\1\6\16\0\3\uffff";
    static final String DFA7_maxS =
        "\1\77\16\0\3\uffff";
    static final String DFA7_acceptS =
        "\17\uffff\1\1\1\2\1\3";
    static final String DFA7_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\3\uffff}>";
    static final String[] DFA7_transitionS = {
            "\1\5\1\14\1\11\1\uffff\1\10\1\12\22\uffff\1\4\25\uffff\1\1\1"+
            "\2\2\3\2\uffff\1\16\1\uffff\1\6\1\7\1\13\1\15",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            ""
    };

    static final short[] DFA7_eot = DFA.unpackEncodedString(DFA7_eotS);
    static final short[] DFA7_eof = DFA.unpackEncodedString(DFA7_eofS);
    static final char[] DFA7_min = DFA.unpackEncodedStringToUnsignedChars(DFA7_minS);
    static final char[] DFA7_max = DFA.unpackEncodedStringToUnsignedChars(DFA7_maxS);
    static final short[] DFA7_accept = DFA.unpackEncodedString(DFA7_acceptS);
    static final short[] DFA7_special = DFA.unpackEncodedString(DFA7_specialS);
    static final short[][] DFA7_transition;

    static {
        int numStates = DFA7_transitionS.length;
        DFA7_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA7_transition[i] = DFA.unpackEncodedString(DFA7_transitionS[i]);
        }
    }

    class DFA7 extends DFA {

        public DFA7(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 7;
            this.eot = DFA7_eot;
            this.eof = DFA7_eof;
            this.min = DFA7_min;
            this.max = DFA7_max;
            this.accept = DFA7_accept;
            this.special = DFA7_special;
            this.transition = DFA7_transition;
        }
        public String getDescription() {
            return "121:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA7_1 = input.LA(1);

                         
                        int index7_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA7_2 = input.LA(1);

                         
                        int index7_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA7_3 = input.LA(1);

                         
                        int index7_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA7_4 = input.LA(1);

                         
                        int index7_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA7_5 = input.LA(1);

                         
                        int index7_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA7_6 = input.LA(1);

                         
                        int index7_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA7_7 = input.LA(1);

                         
                        int index7_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA7_8 = input.LA(1);

                         
                        int index7_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA7_9 = input.LA(1);

                         
                        int index7_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA7_10 = input.LA(1);

                         
                        int index7_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA7_11 = input.LA(1);

                         
                        int index7_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA7_12 = input.LA(1);

                         
                        int index7_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA7_13 = input.LA(1);

                         
                        int index7_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA7_14 = input.LA(1);

                         
                        int index7_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_Meteor()) ) {s = 15;}

                        else if ( (synpred11_Meteor()) ) {s = 16;}

                        else if ( (true) ) {s = 17;}

                         
                        input.seek(index7_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 7, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA22_eotS =
        "\17\uffff";
    static final String DFA22_eofS =
        "\17\uffff";
    static final String DFA22_minS =
        "\1\6\13\0\3\uffff";
    static final String DFA22_maxS =
        "\1\77\13\0\3\uffff";
    static final String DFA22_acceptS =
        "\14\uffff\1\1\1\2\1\3";
    static final String DFA22_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\3\uffff}>";
    static final String[] DFA22_transitionS = {
            "\1\2\1\11\1\6\1\uffff\1\5\1\7\22\uffff\1\1\33\uffff\1\13\1\uffff"+
            "\1\3\1\4\1\10\1\12",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            ""
    };

    static final short[] DFA22_eot = DFA.unpackEncodedString(DFA22_eotS);
    static final short[] DFA22_eof = DFA.unpackEncodedString(DFA22_eofS);
    static final char[] DFA22_min = DFA.unpackEncodedStringToUnsignedChars(DFA22_minS);
    static final char[] DFA22_max = DFA.unpackEncodedStringToUnsignedChars(DFA22_maxS);
    static final short[] DFA22_accept = DFA.unpackEncodedString(DFA22_acceptS);
    static final short[] DFA22_special = DFA.unpackEncodedString(DFA22_specialS);
    static final short[][] DFA22_transition;

    static {
        int numStates = DFA22_transitionS.length;
        DFA22_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA22_transition[i] = DFA.unpackEncodedString(DFA22_transitionS[i]);
        }
    }

    class DFA22 extends DFA {

        public DFA22(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 22;
            this.eot = DFA22_eot;
            this.eof = DFA22_eof;
            this.min = DFA22_min;
            this.max = DFA22_max;
            this.accept = DFA22_accept;
            this.special = DFA22_special;
            this.transition = DFA22_transition;
        }
        public String getDescription() {
            return "172:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA22_1 = input.LA(1);

                         
                        int index22_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_Meteor()) ) {s = 12;}

                        else if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA22_2 = input.LA(1);

                         
                        int index22_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA22_3 = input.LA(1);

                         
                        int index22_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA22_4 = input.LA(1);

                         
                        int index22_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA22_5 = input.LA(1);

                         
                        int index22_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA22_6 = input.LA(1);

                         
                        int index22_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA22_7 = input.LA(1);

                         
                        int index22_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA22_8 = input.LA(1);

                         
                        int index22_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA22_9 = input.LA(1);

                         
                        int index22_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA22_10 = input.LA(1);

                         
                        int index22_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA22_11 = input.LA(1);

                         
                        int index22_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_Meteor()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index22_11);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 22, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA23_eotS =
        "\16\uffff";
    static final String DFA23_eofS =
        "\16\uffff";
    static final String DFA23_minS =
        "\1\6\13\0\2\uffff";
    static final String DFA23_maxS =
        "\1\77\13\0\2\uffff";
    static final String DFA23_acceptS =
        "\14\uffff\1\1\1\2";
    static final String DFA23_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\2\uffff}>";
    static final String[] DFA23_transitionS = {
            "\1\1\1\11\1\6\1\uffff\1\5\1\7\22\uffff\1\2\33\uffff\1\13\1\uffff"+
            "\1\3\1\4\1\10\1\12",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            ""
    };

    static final short[] DFA23_eot = DFA.unpackEncodedString(DFA23_eotS);
    static final short[] DFA23_eof = DFA.unpackEncodedString(DFA23_eofS);
    static final char[] DFA23_min = DFA.unpackEncodedStringToUnsignedChars(DFA23_minS);
    static final char[] DFA23_max = DFA.unpackEncodedStringToUnsignedChars(DFA23_maxS);
    static final short[] DFA23_accept = DFA.unpackEncodedString(DFA23_acceptS);
    static final short[] DFA23_special = DFA.unpackEncodedString(DFA23_specialS);
    static final short[][] DFA23_transition;

    static {
        int numStates = DFA23_transitionS.length;
        DFA23_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA23_transition[i] = DFA.unpackEncodedString(DFA23_transitionS[i]);
        }
    }

    class DFA23 extends DFA {

        public DFA23(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 23;
            this.eot = DFA23_eot;
            this.eof = DFA23_eof;
            this.min = DFA23_min;
            this.max = DFA23_max;
            this.accept = DFA23_accept;
            this.special = DFA23_special;
            this.transition = DFA23_transition;
        }
        public String getDescription() {
            return "178:1: generalPathExpression : (value= valueExpression path= pathExpression -> | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA23_1 = input.LA(1);

                         
                        int index23_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA23_2 = input.LA(1);

                         
                        int index23_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA23_3 = input.LA(1);

                         
                        int index23_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA23_4 = input.LA(1);

                         
                        int index23_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA23_5 = input.LA(1);

                         
                        int index23_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA23_6 = input.LA(1);

                         
                        int index23_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA23_7 = input.LA(1);

                         
                        int index23_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA23_8 = input.LA(1);

                         
                        int index23_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA23_9 = input.LA(1);

                         
                        int index23_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA23_10 = input.LA(1);

                         
                        int index23_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA23_11 = input.LA(1);

                         
                        int index23_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_Meteor()) ) {s = 12;}

                        else if ( (true) ) {s = 13;}

                         
                        input.seek(index23_11);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 23, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA25_eotS =
        "\16\uffff";
    static final String DFA25_eofS =
        "\1\uffff\1\10\2\uffff\1\11\11\uffff";
    static final String DFA25_minS =
        "\2\6\2\uffff\1\6\5\uffff\1\6\1\43\1\uffff\1\0";
    static final String DFA25_maxS =
        "\1\77\1\100\2\uffff\1\100\5\uffff\1\77\1\100\1\uffff\1\0";
    static final String DFA25_acceptS =
        "\2\uffff\1\2\1\3\1\uffff\1\7\1\10\1\1\1\5\1\4\2\uffff\1\6\1\uffff";
    static final String DFA25_specialS =
        "\15\uffff\1\0}>";
    static final String[] DFA25_transitionS = {
            "\1\1\1\4\1\3\1\uffff\2\3\22\uffff\1\2\33\uffff\1\6\1\uffff\3"+
            "\3\1\5",
            "\1\10\2\uffff\1\10\20\uffff\1\10\3\uffff\1\7\2\10\1\uffff\22"+
            "\10\4\uffff\2\10\1\uffff\1\10\3\uffff\2\10",
            "",
            "",
            "\1\11\2\uffff\1\11\20\uffff\1\11\4\uffff\2\11\1\uffff\22\11"+
            "\4\uffff\2\11\1\uffff\1\11\3\uffff\1\12\1\11",
            "",
            "",
            "",
            "",
            "",
            "\3\14\1\11\1\14\1\13\1\11\21\uffff\1\14\33\uffff\1\14\1\uffff"+
            "\4\14",
            "\1\11\25\uffff\1\14\5\uffff\1\14\1\15",
            "",
            "\1\uffff"
    };

    static final short[] DFA25_eot = DFA.unpackEncodedString(DFA25_eotS);
    static final short[] DFA25_eof = DFA.unpackEncodedString(DFA25_eofS);
    static final char[] DFA25_min = DFA.unpackEncodedStringToUnsignedChars(DFA25_minS);
    static final char[] DFA25_max = DFA.unpackEncodedStringToUnsignedChars(DFA25_maxS);
    static final short[] DFA25_accept = DFA.unpackEncodedString(DFA25_acceptS);
    static final short[] DFA25_special = DFA.unpackEncodedString(DFA25_specialS);
    static final short[][] DFA25_transition;

    static {
        int numStates = DFA25_transitionS.length;
        DFA25_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA25_transition[i] = DFA.unpackEncodedString(DFA25_transitionS[i]);
        }
    }

    class DFA25 extends DFA {

        public DFA25(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 25;
            this.eot = DFA25_eot;
            this.eof = DFA25_eof;
            this.min = DFA25_min;
            this.max = DFA25_max;
            this.accept = DFA25_accept;
            this.special = DFA25_special;
            this.transition = DFA25_transition;
        }
        public String getDescription() {
            return "193:1: valueExpression : ( methodCall[null] | parenthesesExpression | literal | VAR -> | ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA25_13 = input.LA(1);

                         
                        int index25_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred40_Meteor()) ) {s = 9;}

                        else if ( (synpred42_Meteor()) ) {s = 12;}

                         
                        input.seek(index25_13);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 25, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA52_eotS =
        "\12\uffff";
    static final String DFA52_eofS =
        "\1\2\11\uffff";
    static final String DFA52_minS =
        "\1\6\1\0\10\uffff";
    static final String DFA52_maxS =
        "\1\100\1\0\10\uffff";
    static final String DFA52_acceptS =
        "\2\uffff\1\2\6\uffff\1\1";
    static final String DFA52_specialS =
        "\1\uffff\1\0\10\uffff}>";
    static final String[] DFA52_transitionS = {
            "\1\1\23\uffff\1\2\4\uffff\2\2\2\uffff\1\2\27\uffff\1\2\4\uffff"+
            "\1\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA52_eot = DFA.unpackEncodedString(DFA52_eotS);
    static final short[] DFA52_eof = DFA.unpackEncodedString(DFA52_eofS);
    static final char[] DFA52_min = DFA.unpackEncodedStringToUnsignedChars(DFA52_minS);
    static final char[] DFA52_max = DFA.unpackEncodedStringToUnsignedChars(DFA52_maxS);
    static final short[] DFA52_accept = DFA.unpackEncodedString(DFA52_acceptS);
    static final short[] DFA52_special = DFA.unpackEncodedString(DFA52_specialS);
    static final short[][] DFA52_transition;

    static {
        int numStates = DFA52_transitionS.length;
        DFA52_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA52_transition[i] = DFA.unpackEncodedString(DFA52_transitionS[i]);
        }
    }

    class DFA52 extends DFA {

        public DFA52(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 52;
            this.eot = DFA52_eot;
            this.eof = DFA52_eof;
            this.min = DFA52_min;
            this.max = DFA52_max;
            this.accept = DFA52_accept;
            this.special = DFA52_special;
            this.transition = DFA52_transition;
        }
        public String getDescription() {
            return "329:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA52_1 = input.LA(1);

                         
                        int index52_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred77_Meteor()) ) {s = 9;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index52_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 52, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_statement_in_script125 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_script127 = new BitSet(new long[]{0x00000000080000C2L,0x000000000000000AL});
    public static final BitSet FOLLOW_assignment_in_statement141 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement145 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_packageImport_in_statement149 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionDefinition_in_statement153 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_javaudf_in_statement157 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_27_in_packageImport172 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_packageImport176 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment191 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_assignment193 = new BitSet(new long[]{0x0000000000000040L,0x000000000000000AL});
    public static final BitSet FOLLOW_operator_in_assignment197 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionDefinition219 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_functionDefinition221 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_29_in_functionDefinition223 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_functionDefinition225 = new BitSet(new long[]{0x0000000100000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition234 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_31_in_functionDefinition241 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition245 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_32_in_functionDefinition256 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_functionDefinition268 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_javaudf286 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_javaudf288 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_javaudf290 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_javaudf292 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_javaudf296 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_javaudf298 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_contextAwareExpression326 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_expression336 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_expression342 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression353 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_ternaryExpression356 = new BitSet(new long[]{0xF4F0000840000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_ternaryExpression360 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_ternaryExpression363 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_ternaryExpression367 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression390 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_ternaryExpression392 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_ternaryExpression396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression418 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression431 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_37_in_orExpression435 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_38_in_orExpression439 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_andExpression_in_orExpression444 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression473 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_39_in_andExpression477 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_40_in_andExpression481 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression486 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression515 = new BitSet(new long[]{0x0000060000000002L});
    public static final BitSet FOLLOW_41_in_elementExpression520 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_elementExpression523 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression527 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression568 = new BitSet(new long[]{0x0001F80000000002L});
    public static final BitSet FOLLOW_43_in_comparisonExpression574 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_44_in_comparisonExpression580 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_45_in_comparisonExpression586 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_46_in_comparisonExpression592 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_47_in_comparisonExpression598 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_48_in_comparisonExpression604 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression609 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression689 = new BitSet(new long[]{0x0006000000000002L});
    public static final BitSet FOLLOW_49_in_arithmeticExpression695 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_50_in_arithmeticExpression701 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression706 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression749 = new BitSet(new long[]{0x0008000000000202L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression755 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_51_in_multiplicationExpression761 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_52_in_preincrementExpression807 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression809 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_53_in_preincrementExpression814 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression816 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression821 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression831 = new BitSet(new long[]{0xF4F0000040000DC0L});
    public static final BitSet FOLLOW_castExpression_in_unaryExpression840 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_castExpression850 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression854 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_castExpression856 = new BitSet(new long[]{0xF400000040000DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression860 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression867 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_castExpression869 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression873 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression880 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression907 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_generalPathExpression911 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression921 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_contextAwarePathExpression934 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_pathExpression967 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_pathExpression972 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression990 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_methodCall_in_valueExpression1011 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression1017 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression1023 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression1029 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_valueExpression1039 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streamIndexAccess_in_valueExpression1052 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression1057 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression1063 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression1076 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_parenthesesExpression1097 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression1099 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_parenthesesExpression1101 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_methodCall1124 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_methodCall1126 = new BitSet(new long[]{0xF4F0000140000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_methodCall1133 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_31_in_methodCall1139 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_methodCall1143 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_32_in_methodCall1153 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1167 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1169 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1171 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1186 = new BitSet(new long[]{0x8200000010000000L});
    public static final BitSet FOLLOW_57_in_fieldAssignment1195 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment1197 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_28_in_fieldAssignment1211 = new BitSet(new long[]{0x0000000000000040L,0x000000000000000AL});
    public static final BitSet FOLLOW_operator_in_fieldAssignment1215 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_fieldAssignment1230 = new BitSet(new long[]{0x0000000800000002L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1241 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1245 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_objectCreation1297 = new BitSet(new long[]{0x08000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1300 = new BitSet(new long[]{0x0800000080000000L});
    public static final BitSet FOLLOW_31_in_objectCreation1303 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1305 = new BitSet(new long[]{0x0800000080000000L});
    public static final BitSet FOLLOW_31_in_objectCreation1309 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_objectCreation1314 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_literal1334 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_literal1350 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal1366 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal1382 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal1399 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_62_in_literal1414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1428 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1430 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1432 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_arrayAccess1436 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1456 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1461 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1467 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1470 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1488 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1493 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1499 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_arrayAccess1502 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1507 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1513 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1516 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_streamIndexAccess1544 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_63_in_streamIndexAccess1553 = new BitSet(new long[]{0xF400000040000DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_streamIndexAccess1557 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_streamIndexAccess1559 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayCreation1578 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_arrayCreation1582 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayCreation1585 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_arrayCreation1589 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayCreation1593 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayCreation1596 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1633 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1637 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1641 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_65_in_readOperator1655 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_66_in_readOperator1657 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_readOperator1662 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1667 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1673 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_readOperator1675 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1679 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_readOperator1681 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_67_in_writeOperator1695 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1699 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_68_in_writeOperator1701 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_writeOperator1706 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1711 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1717 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_writeOperator1719 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1723 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_writeOperator1725 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1745 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_operatorFlag_in_genericOperator1753 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_arrayInput_in_genericOperator1757 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_input_in_genericOperator1761 = new BitSet(new long[]{0x0000000080000042L});
    public static final BitSet FOLLOW_31_in_genericOperator1764 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_input_in_genericOperator1766 = new BitSet(new long[]{0x0000000080000042L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1772 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorOption1792 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_operatorOption1798 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1819 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_69_in_input1841 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1849 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_input1851 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1857 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_input1867 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_input1875 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayInput1897 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput1901 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayInput1904 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput1908 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayInput1912 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_arrayInput1914 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput1918 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_synpred8_Meteor336 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred10_Meteor353 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_synpred10_Meteor356 = new BitSet(new long[]{0xF4F0000840000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_synpred10_Meteor360 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_synpred10_Meteor363 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_synpred10_Meteor367 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred11_Meteor390 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_synpred11_Meteor392 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_expression_in_synpred11_Meteor396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_synpred32_Meteor850 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred32_Meteor854 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_synpred32_Meteor856 = new BitSet(new long[]{0xF400000040000DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred32_Meteor860 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred33_Meteor867 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_synpred33_Meteor869 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred33_Meteor873 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred34_Meteor907 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_synpred34_Meteor911 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_synpred35_Meteor967 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred35_Meteor972 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred36_Meteor990 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred40_Meteor1029 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streamIndexAccess_in_synpred42_Meteor1052 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_31_in_synpred73_Meteor1764 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000020L});
    public static final BitSet FOLLOW_input_in_synpred73_Meteor1766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred74_Meteor1772 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred77_Meteor1867 = new BitSet(new long[]{0xF4F0000040000DC0L,0x000000000000000AL});
    public static final BitSet FOLLOW_contextAwareExpression_in_synpred77_Meteor1875 = new BitSet(new long[]{0x0000000000000002L});

}