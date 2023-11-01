public class TestIfStatementRewrite2 {
    public void myFun1(int[] a, int[] b) throws RuntimeException {
        if (a[0] == 0) {
            a[0] = 1;
            if (a[1] == 0) {
                a[1] = 1;
                if (a[2] == 0) {
                    a[2] = 1;
                } else if (a[3] == 0) {
                    a[3] = b[3];
                    a[33] = b[33];
                } else if (a[4] == 0) {
                    a[4] = b[4];
                    a[44] = b[44];
                } else {
                    a[5] = 5;
                    System.out.println("nothing");
                }
            } else {
                a[1] = b[1];
                a[2] = b[2];
            }
        } else if(a[1] == 22) {
            a[1] = b[12];
            a[2] = b[22];
        } else {
            a[0] = b[0];
            a[1] = b[1];
            a[2] = b[2];
        }
    }
}
