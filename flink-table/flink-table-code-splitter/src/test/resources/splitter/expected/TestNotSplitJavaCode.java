public class TestNotSplitJavaCode {
    private int a = 1;
    public final int[] b;

    public TestSplitJavaCode(int[] b) {
        this.b = b;
    }

    public void myFun(int a) {
        this.a = a;
        b[0] += b[1];
        b[1] += b[2];
        b[2] += b[3];
        int b1 = b[0] + b[1];
        int b2 = b[1] + b[2];
        int b3 = b[2] + b[0];
        for (int x : b) {
            System.out.println(x + b1 + b2 + b3);
        }

        if (b1 > 0) {
            b[0] += 100;
            if (b2 > 0) {
                b[1] += 100;
                if (b3 > 0) {
                    b[2] += 100;
                } else {
                    b[2] += 50;
                }
            } else {
                b[1] += 50;
            }
        } else {
            b[0] += 50;
        }
    }
}
