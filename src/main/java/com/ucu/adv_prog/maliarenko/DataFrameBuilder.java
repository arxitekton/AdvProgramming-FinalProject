package com.ucu.adv_prog.maliarenko;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
                    System.out.println(line);

                    Map<String, String> map = Splitter.on( ";" )
                            .omitEmptyStrings()
                            .trimResults()
                            .withKeyValueSeparator( '=' )
                            .split( line );

                    System.out.println(map);
                    System.out.println(map.get("from"));

                    MatchEvent event = MatchEvent.builder()
                        .code(map.get("code"))
                        .from(map.get("from"))
                        .to(map.get("to"))
                        .eventTime(map.get("eventTime"))
                        .stadion(map.get("stadion")).build();

                    return event;
        });

        //JavaRDD<Row> rowRDD = eventRdd.map(event -> RowFactory.create(event.getCode(), event.getCityName(), event.getKm()));

        return sqlContext.createDataFrame(eventRdd, MatchEvent.class);


    }


    private static StructType createSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("country code", DataTypes.IntegerType, false),
                DataTypes.createStructField("amount", DataTypes.IntegerType, false),
                DataTypes.createStructField("account number", DataTypes.IntegerType, false)
        });
    }











}
