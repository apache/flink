public class TestIfStatementRewrite1 {
    public void myFun1(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
            System.out.println("0");
            System.out.println("0");
            if (a[1] == 0) {
                System.out.println("1");
                System.out.println("2");
                if (a[2] == 0) {
                    a[2] = 1;
                    a[22] = 1;
                } else {
                    a[2] = b[2];
                    a[22] = b[2];
                }
            } else {
                a[1] = b[1];
                a[2] = b[2];
            }
        } else {
            System.out.println("3");
            System.out.println("3");
            if (a[1] == 1) {
                a[0] = b[0];
                a[1] = b[1];
                a[2] = b[2];
            } else {
                a[0] = 2 * b[0];
                a[1] = 2 * b[1];
                a[2] = 2 * b[2];
            }
        }
    }
}
