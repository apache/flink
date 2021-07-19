public class TestRewriteReturnValue {
String codeSplitReturnValue$0;String codeSplitReturnValue$1;
    public String fun1(int a, String b) { fun1_impl(a, b); return codeSplitReturnValue$0; }

void fun1_impl(int a, String b) {
        if (a > 0) {
            a += 5;
            { codeSplitReturnValue$0 = b + "test" + a; return; }
        }
        a -= 5;
        { codeSplitReturnValue$0 = b + "test" + a; return; }
    }

    public String fun2(int a, String b) throws Exception { fun2_impl(a, b); return codeSplitReturnValue$1; }

void fun2_impl(int a, String b) throws Exception {
        if (a > 0) {
            a += 5;
            if (a > 100) {
                throw new RuntimeException();
            }
            { codeSplitReturnValue$1 = b + "test" + a; return; }
        }
        a -= 5;
        { codeSplitReturnValue$1 = b + "test" + a; return; }
    }
}
