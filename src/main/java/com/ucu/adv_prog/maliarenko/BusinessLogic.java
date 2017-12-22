package com.ucu.adv_prog.maliarenko;

import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
public class BusinessLogic {

    @Autowired
    private DataFrameBuilder dataFrameBuilder;



    public void doEnrichment(){
        DataFrame dataFrame = dataFrameBuilder.load();
        dataFrame=dataFrame
                .withColumn("code_description", callUDF(CodesConverter.class.getName(),col("code")))
                .withColumn("fromTeam", callUDF(TeamDetection.class.getName(),col("from")))
                .withColumn("toTeam", callUDF(TeamDetection.class.getName(),col("to")));
        dataFrame.show();

    }
}
