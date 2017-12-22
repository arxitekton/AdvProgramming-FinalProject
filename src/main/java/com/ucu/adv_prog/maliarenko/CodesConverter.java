package com.ucu.adv_prog.maliarenko;

import com.ucu.adv_prog.maliarenko.AutowiredBroadcast;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;


@RegisterUDF
public class CodesConverter implements UDF1<String,String>, Serializable {

    @AutowiredBroadcast
    private Broadcast<CodesConfig> broadcast;

    @Override
    public String call(String countryCode) throws Exception {
        return broadcast.value().getMap().get(countryCode);
    }
}


