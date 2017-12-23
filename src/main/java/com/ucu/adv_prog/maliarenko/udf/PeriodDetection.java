package com.ucu.adv_prog.maliarenko.udf;

import com.ucu.adv_prog.maliarenko.AutowiredBroadcast;
import com.ucu.adv_prog.maliarenko.RegisterUDF;
import com.ucu.adv_prog.maliarenko.config.TeamsConfig;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;


@RegisterUDF
public class PeriodDetection implements UDF1<String,String>, Serializable {


    @Override
    public String call(String eventTime) throws Exception {
        Calendar cal = Calendar.getInstance();

        SimpleDateFormat sdf = new SimpleDateFormat("mm:ss");
        Date date = sdf.parse(eventTime);
        cal.setTime(date);

        int seconds = cal.get(Calendar.HOUR)*3600 + cal.get(Calendar.MINUTE)*60 + cal.get(Calendar.SECOND);

        //int time = seconds / 2700;

        if (seconds<=2700) {
            return "1 time";
        } else if (seconds<=5400) {
            return "2 time";
        } else if (seconds<=6300) {
            return "1st overtime (15 min)";
        } else if (seconds<=7200) {
            return "2st overtime (15 min)";
        }

        return null;
    }
}


