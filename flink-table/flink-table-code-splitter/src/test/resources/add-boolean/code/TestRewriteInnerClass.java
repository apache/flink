public class TestRewriteInnerClass {
    public void fun(int a) {
        if (a > 0) {
            a += 5;
            return;
        }
        a -= 5;
        return;
    }

    public class InnerClass1 {
        public void fun(int a) {
            if (a > 0) {
                a += 5;
                return;
            }
            a -= 5;
            return;
        }
    }

    public class InnerClass2 {
        public void fun(int a) {
            if (a > 0) {
                a += 5;
                return;
            }
            a -= 5;
            return;
        }
    }
}
