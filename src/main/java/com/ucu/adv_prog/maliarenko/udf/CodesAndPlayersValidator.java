package com.ucu.adv_prog.maliarenko.udf;

import com.ucu.adv_prog.maliarenko.registerUDF.RegisterUDF3;
import org.apache.spark.sql.api.java.UDF3;

import java.io.Serializable;
import java.util.stream.IntStream;


@RegisterUDF3
public class CodesAndPlayersValidator implements UDF3<String,String,String,Boolean>, Serializable {

    @Override
    public Boolean call(String code, String playerFrom, String playerTo) throws Exception {

        // only playerFrom
        int[] codesWithFromOnly = {5,6,10};

        // only playerTo
        int[] codesWithToOnly = {2};

        boolean containsInCodesWithFromOnly = IntStream.of(codesWithFromOnly).anyMatch(x -> x == Integer.parseInt(code));
        boolean containsInCodesWithToOnly = IntStream.of(codesWithToOnly).anyMatch(x -> x == Integer.parseInt(code));

        if (containsInCodesWithFromOnly && playerFrom!=null && playerTo==null) {
            return true;
        } else if (containsInCodesWithToOnly && playerFrom==null && playerTo!=null) {
            return true;
        } else if (!containsInCodesWithFromOnly && !containsInCodesWithToOnly &&  playerFrom!=null && playerTo!=null) {
            return true;
        }

        return false;
    }
}


