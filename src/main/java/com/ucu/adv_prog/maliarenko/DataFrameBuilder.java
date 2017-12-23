package com.ucu.adv_prog.maliarenko;

import com.ucu.adv_prog.maliarenko.model.MatchEvent;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.base.Splitter;

import java.util.Map;

@Service
public class DataFrameBuilder {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private SQLContext sqlContext;

    @Value("${rawdata.path}")
    private String rawdataPath;

    public DataFrame load() {
        JavaRDD<String> rdd = sc.textFile(rawdataPath);
        JavaRDD<MatchEvent> eventRdd = rdd
                .filter(line -> line.length()!=0)
                .map(line -> {

                    Map<String, String> map = Splitter.on( ";" )
                            .omitEmptyStrings()
                            .trimResults()
                            .withKeyValueSeparator( '=' )
                            .split( line );

                    MatchEvent event = MatchEvent.builder()
                        .code(map.get("code"))
                        .from(map.get("from"))
                        .to(map.get("to"))
                        .eventTime(map.get("eventTime"))
                        .stadion(map.get("stadion")).build();

                    return event;
        });

        return sqlContext.createDataFrame(eventRdd, MatchEvent.class);


    }


}
