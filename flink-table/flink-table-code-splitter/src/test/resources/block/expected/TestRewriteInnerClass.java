public class TestRewriteInnerClass {
    public void myFun(int[] a, int[] b) {

        if (a[0] == 0) {
            myFun_0(a, b);
        } else {
            myFun_1(a, b);
        }
    }

    void myFun_0(int[] a, int[] b) {
        a[0] += b[0];
        a[1] += b[1];
    }

    void myFun_1(int[] a, int[] b) {
        a[0] += b[1];
        a[1] += b[0];
    }


    public class InnerClass {
        public void myFun(int[] a, int[] b) {

            if (a[0] == 0) {
                myFun_0(a, b);
            } else {
                myFun_1(a, b);
            }
        }

        void myFun_0(int[] a, int[] b) {
            a[0] += b[0];
            a[1] += b[1];
        }

        void myFun_1(int[] a, int[] b) {
            a[0] += b[1];
            a[1] += b[0];
        }

    }
}
