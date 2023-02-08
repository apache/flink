package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

public class array_cat extends ScalarFunction {

    public List<String> eval(List<String> ar1, List<String> ar2) {
        if (ar1 == null && ar2 == null) {
            return null;
        }

        ArrayList<String> res = new ArrayList<String>();

        if (ar1 != null) {
            for (String v : ar1) {
                res.add(v);
            }
        }

        if (ar2 != null) {
            for (String v : ar2) {
                res.add(v);
            }
        }

        return res;
    }

    public List<String> eval(List<String> ar1, String argScalar) {
        if (ar1 == null && argScalar == null) {
            return null;
        }

        ArrayList<String> res;
        if (ar1 == null) {
            res = new ArrayList<String>();
        } else {
            res = new ArrayList<String>(ar1);
        }

        if (argScalar != null) {
            res.add(argScalar);
        }

        return res;
    }
}
