public class TestRewriteInnerClass {
String codeSplitReturnValue$0;
    public String fun(int a, String b) { fun_impl(a, b); return codeSplitReturnValue$0; }

void fun_impl(int a, String b) {
        if (a > 0) {
            a += 5;
            { codeSplitReturnValue$0 = b + "test" + a; return; }
        }
        a -= 5;
        { codeSplitReturnValue$0 = b + "test" + a; return; }
    }

    public class InnerClass {
String codeSplitReturnValue$1;
        public String fun(int a, String b) { fun_impl(a, b); return codeSplitReturnValue$1; }

void fun_impl(int a, String b) {
            if (a > 0) {
                a += 5;
                { codeSplitReturnValue$1 = b + "test" + a; return; }
            }
            a -= 5;
            { codeSplitReturnValue$1 = b + "test" + a; return; }
        }
    }
}
