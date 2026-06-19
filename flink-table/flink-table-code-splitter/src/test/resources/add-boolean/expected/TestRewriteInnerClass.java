public class TestRewriteInnerClass {
boolean funHasReturned$0;
    public void fun(int a) {
        if (a > 0) {
            a += 5;
            { funHasReturned$0 = true; return; }
        }
        a -= 5;
        { funHasReturned$0 = true; return; }
    }

    public class InnerClass1 {
boolean funHasReturned$1;
        public void fun(int a) {
            if (a > 0) {
                a += 5;
                { funHasReturned$1 = true; return; }
            }
            a -= 5;
            { funHasReturned$1 = true; return; }
        }
    }

    public class InnerClass2 {
boolean funHasReturned$2;
        public void fun(int a) {
            if (a > 0) {
                a += 5;
                { funHasReturned$2 = true; return; }
            }
            a -= 5;
            { funHasReturned$2 = true; return; }
        }
    }
}
