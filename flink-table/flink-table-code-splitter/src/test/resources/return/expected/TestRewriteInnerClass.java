public class TestRewriteInnerClass {
String funReturnValue$0;

    public String fun(int a, String b) { funImpl(a, b); return funReturnValue$0; }

void funImpl(int a, String b) {
        if (a > 0) {
            a += 5;
            { funReturnValue$0 = b + "test" + a; return; }
        }
        a -= 5;
        { funReturnValue$0 = b + "test" + a; return; }
    }

    public class InnerClass {
String funReturnValue$1;

        public String fun(int a, String b) { funImpl(a, b); return funReturnValue$1; }

void funImpl(int a, String b) {
            if (a > 0) {
                a += 5;
                { funReturnValue$1 = b + "test" + a; return; }
            }
            a -= 5;
            { funReturnValue$1 = b + "test" + a; return; }
        }
    }
}
