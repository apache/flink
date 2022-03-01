public class TestRewriteInnerClass {
    public String fun(int a, String b) {
        if (a > 0) {
            a += 5;
            return b + "test" + a;
        }
        a -= 5;
        return b + "test" + a;
    }

    public class InnerClass {
        public String fun(int a, String b) {
            if (a > 0) {
                a += 5;
                return b + "test" + a;
            }
            a -= 5;
            return b + "test" + a;
        }
    }
}
