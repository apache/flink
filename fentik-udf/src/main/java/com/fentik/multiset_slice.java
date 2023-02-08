package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class multiset_slice extends ScalarFunction {

    public @DataTypeHint("MULTISET<STRING>") Map<String, Integer> eval(
            @DataTypeHint("MULTISET<STRING>") Map<String, Integer> m,
            Integer from, Integer to) {
        if (m == null) {
            return null;
        }

        if (from == null) {
            from = Integer.valueOf(Integer.MAX_VALUE);
        }

        if (to == null) {
            to = Integer.valueOf(Integer.MIN_VALUE);
        }

        Map<String, Integer> res = new HashMap<String, Integer>();
        int i = 0;

        // we want deterministic behavior out of this function, so we
        // sort the hash set to prevent spurious changes from call to
        // call which can result in amplification in the flink job
        List<String> sortedKeys = new ArrayList<>(m.keySet());

        for (String key : sortedKeys) {
            if (i < from.intValue()) {
                continue;
            }
            if (i >= to.intValue()) {
                break;
            }
            res.put(key, m.get(key));
            i++;
        }

        return res;
    }
}
