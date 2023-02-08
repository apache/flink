package com.fentik;

import com.google.gson.Gson;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.Arrays;
import java.util.List;

public class array_from_json extends ScalarFunction {
    private static final Gson gson = new Gson();

    public List<String> eval(String maybeJSON) {
        if (maybeJSON == null) {
            return null;
        }

        try {
            String[] ret = gson.fromJson(maybeJSON, String[].class);
            return Arrays.asList(ret);
        } catch (Exception e) {
            // on error, customer wants NULL
            return null;
        }
    }

}
