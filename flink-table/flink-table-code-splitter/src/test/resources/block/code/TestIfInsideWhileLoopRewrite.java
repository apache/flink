public class TestIfInsideWhileLoopRewrite {

    int counter = 0;

    public void myFun(int[] a, int[] b, int[] c ) throws RuntimeException {

        a[0] += b[1];
        b[1] += a[1];
        while (counter < 10) {
            c[counter] = a[0] + 1000;
            System.out.println(c);
            if (a[counter] > 0) {
                b[counter] = a[counter] * 2;
                c[counter] = b[counter] * 2;
                System.out.println(b[counter]);
            } else {
                b[counter] = a[counter] * 3;
                System.out.println(b[counter]);
            }

            a[2] += b[2];
            b[3] += a[3];
            if (a[0] > 0) {
                System.out.println("Hello");
            } else {
                System.out.println("World");
            }

            counter--;
        }

        a[4] += b[4];
        b[5] += a[5];
    }

    public void myFun2(int[] a, int[] b, int[] c) throws RuntimeException {

        a[0] += b[1];
        b[1] += a[1];
        while (counter < 10) {
            c[counter] = a[0] + 1000;
            System.out.println(c);
            if (a[counter] > 0) {
                b[counter] = a[counter] * 2;
                c[counter] = b[counter] * 2;
                System.out.println(b[counter]);
            } else {
                b[counter] = a[counter] * 3;
                System.out.println(b[counter]);
            }

            a[2] += b[2];
            b[3] += a[3];
            if (a[0] > 0) {
                System.out.println("Hello");
            } else {
                System.out.println("World");
                break;
            }

            counter--;
        }

        a[4] += b[4];
        b[5] += a[5];
    }

    public void myFun3(int[] a, int[] b, int[] c) throws RuntimeException {

        a[0] += b[1];
        b[1] += a[1];
        while (counter < 10) {
            c[counter] = a[0] + 1000;
            System.out.println(c);
            if (a[counter] > 0) {
                b[counter] = a[counter] * 2;
                c[counter] = b[counter] * 2;
                System.out.println(b[counter]);
            } else {
                b[counter] = a[counter] * 3;
                System.out.println(b[counter]);
                continue;
            }

            a[2] += b[2];
            b[3] += a[3];
            if (a[0] > 0) {
                System.out.println("Hello");
            } else {
                System.out.println("World");
                break;
            }

            counter--;
        }

        a[4] += b[4];
        b[5] += a[5];
    }
}
