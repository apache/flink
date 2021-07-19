public class TestRewriteInnerClass {
boolean codeSplitHasReturned$0;
    public void fun(int a) {
        if (a > 0) {
            a += 5;
            { codeSplitHasReturned$0 = true; return; }
        }
        a -= 5;
        { codeSplitHasReturned$0 = true; return; }
    }

    public class InnerClass1 {
boolean codeSplitHasReturned$1;
        public void fun(int a) {
            if (a > 0) {
                a += 5;
                { codeSplitHasReturned$1 = true; return; }
            }
            a -= 5;
            { codeSplitHasReturned$1 = true; return; }
        }
    }

    public class InnerClass2 {
boolean codeSplitHasReturned$2;
        public void fun(int a) {
            if (a > 0) {
                a += 5;
                { codeSplitHasReturned$2 = true; return; }
            }
            a -= 5;
            { codeSplitHasReturned$2 = true; return; }
        }
    }
}
