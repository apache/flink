public class TestSplitJavaCode {
    private int a = 1;
    public final int[] b;

    public TestSplitJavaCode(int[] b) {
        this.b = b;
        int b0 = this.b[0] + b[0];
        System.out.println("b0 = " + b0);
        int b1 = this.b[1] + b[1];
        System.out.println("b1 = " + b1);
        int b2 = this.b[2] + b[2];
        System.out.println("b2 = " + b2);
        int b3 = this.b[3] + b[3];
        System.out.println("b3 = " + b3);
        int b4 = this.b[4] + b[4];
        System.out.println("b4 = " + b4);
        int b5 = this.b[5] + b[5];
        System.out.println("b5 = " + b5);
        int b6 = this.b[6] + b[6];
        System.out.println("b6 = " + b6);
        int b7 = this.b[7] + b[7];
        System.out.println("b7 = " + b7);
        int b8 = this.b[8] + b[8];
        System.out.println("b8 = " + b8);
        int b9 = this.b[9] + b[9];
        System.out.println("b9 = " + b9);
    }

    public void myFun1(int a) {
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
            b[1] += 100;
            b[2] += 150;
            b[3] += 200;
            b[4] += 250;
            b[5] += 300;
            b[6] += 350;
            return;
        }
    }

    public int myFun2(int[] a) {
        a[0] += 1;
        a[1] += 2;
        a[2] += 3;
        a[3] += 4;
        a[4] += 5;
        return a[0] + a[1] + a[2] + a[3] + a[4];
    }
}
