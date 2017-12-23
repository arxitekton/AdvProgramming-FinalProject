package com.ucu.adv_prog.maliarenko.udf;

import com.ucu.adv_prog.maliarenko.broadcast.AutowiredBroadcast;
import com.ucu.adv_prog.maliarenko.registerUDF.RegisterUDF;
import com.ucu.adv_prog.maliarenko.config.TeamsConfig;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@RegisterUDF
public class TeamDetection implements UDF1<String,String>, Serializable {

    @AutowiredBroadcast
    private Broadcast<TeamsConfig> broadcast;

    @Override
    public String call(String player) throws Exception {
        Map<String, String> map = broadcast.value().getMap();

        String team;

        for (Map.Entry<String, String> entry : map.entrySet())
        {
            List<String> players = Arrays.asList(entry.getValue().split(","));
            boolean match = players.stream().anyMatch(s -> player.contains(s));
            if (match) {
                return entry.getKey();
            }

        }

        return null;
    }
}


