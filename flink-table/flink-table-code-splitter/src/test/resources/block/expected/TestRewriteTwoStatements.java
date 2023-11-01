public class TestIfInsideWhileLoopRewrite {

    int counter = 0;

    public void myFun(int[] a, int[] b, int[] c ) throws RuntimeException {

        a[0] += b[1];
        b[1] += a[1];

        while (counter < 10) {
            myFun_0_0_rewriteGroup2(a, b, c);

            myFun_0_0_rewriteGroup4(a, b, c);

            counter--;
        }

        a[4] += b[4];
        b[5] += a[5];


        if (a.length < 100) {
            while (counter < 10) {
                myFun_1_0_1_2_3(a, b, c);
                if (a[counter] > 0) {
                    myFun_1_0_1_2_3_4(a, b, c);
                } else {
                    myFun_1_0_1_2_3_5(a, b, c);
                }

                myFun_1_0_1_2_6(a, b, c);
                if (a[0] > 0) {
                    System.out.println("Hello");
                } else {
                    System.out.println("World");
                }

                myFun_1_0_1_2(a, b, c);
            }
        } else {
            myFun_1_0_rewriteGroup2(a, b, c);

            myFun_1_9(a, b, c);
        }
    }

    void myFun_0_0_rewriteGroup4(int[] a, int[] b, int[] c ) throws RuntimeException {
        myFun_0_0_4(a, b, c);
        if (a[0] > 0) {
            System.out.println("Hello");
        } else {
            System.out.println("World");
        }
    }

    void myFun_0_0_1_3(int[] a, int[] b, int[] c ) throws RuntimeException {
        b[counter] = a[counter] * 3;
        System.out.println(b[counter]);
    }

    void myFun_0_0_4(int[] a, int[] b, int[] c ) throws RuntimeException {
        a[2] += b[2];
        b[3] += a[3];
    }

    void myFun_0_0_rewriteGroup2(int[] a, int[] b, int[] c ) throws RuntimeException {
        myFun_0_0_1(a, b, c);
        if (a[counter] > 0) {
            myFun_0_0_1_2(a, b, c);
        } else {
            myFun_0_0_1_3(a, b, c);
        }
    }

    void myFun_0_0_1_2(int[] a, int[] b, int[] c ) throws RuntimeException {
        b[counter] = a[counter] * 2;
        c[counter] = b[counter] * 2;
        System.out.println(b[counter]);
    }

    void myFun_0_0_1(int[] a, int[] b, int[] c ) throws RuntimeException {
        c[counter] = a[0] + 1000;
        System.out.println(c);
    }


    void myFun_1_0_1_2_3_4(int[] a, int[] b, int[] c ) throws RuntimeException {
        b[counter] = a[counter] * 2;
        c[counter] = b[counter] * 2;
        System.out.println(b[counter]);
    }

    void myFun_1_0_1_2(int[] a, int[] b, int[] c ) throws RuntimeException {
        counter--;
        System.out.println("World ffff");
    }

    void myFun_1_0_1_2_3_5(int[] a, int[] b, int[] c ) throws RuntimeException {
        b[counter] = a[counter] * 3;
        System.out.println(b[counter]);
    }

    void myFun_1_0_1_2_3(int[] a, int[] b, int[] c ) throws RuntimeException {
        c[counter] = a[0] + 1000;
        System.out.println(c);
    }

    void myFun_1_0_rewriteGroup2(int[] a, int[] b, int[] c ) throws RuntimeException {
        while (counter < 10) {
            myFun_1_9_10_11(a, b, c);
        }
    }

    void myFun_1_9(int[] a, int[] b, int[] c ) throws RuntimeException {
        System.out.println("World Else");
        System.out.println("World Else 2");
    }

    void myFun_1_0_1_2_6(int[] a, int[] b, int[] c ) throws RuntimeException {
        a[2] += b[2];
        b[3] += a[3];
    }

    void myFun_1_9_10_11(int[] a, int[] b, int[] c ) throws RuntimeException {
        b[counter] = b[counter]++;
        counter++;
    }

}
