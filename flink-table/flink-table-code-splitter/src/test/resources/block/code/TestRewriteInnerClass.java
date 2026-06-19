public class TestRewriteInnerClass {
    public void myFun(int[] a, int[] b) {
        if (a[0] == 0) {
            a[0] += b[0];
            a[1] += b[1];
        } else {
            a[0] += b[1];
            a[1] += b[0];
        }
    }

    public class InnerClass {
        public void myFun(int[] a, int[] b) {
            if (a[0] == 0) {
                a[0] += b[0];
                a[1] += b[1];
            } else {
                a[0] += b[1];
                a[1] += b[0];
            }
        }
    }
}
