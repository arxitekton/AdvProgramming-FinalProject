package com.ucu.adv_prog.maliarenko;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;


@RegisterUDF
public class CodesConverter implements UDF1<Integer,String>, Serializable {

    @AutowiredBroadcast
    private Broadcast<CodesConfig> broadcast;

    @Override
    public String call(Integer countryCode) throws Exception {
        return broadcast.value().getMap().get(countryCode);
    }
}


