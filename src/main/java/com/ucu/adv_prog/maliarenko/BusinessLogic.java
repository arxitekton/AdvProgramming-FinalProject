package com.ucu.adv_prog.maliarenko;

import com.ucu.adv_prog.maliarenko.aop.ShowDataFrameInTheBeginning;
import com.ucu.adv_prog.maliarenko.aop.ShowDataFrameInTheEnd;
import com.ucu.adv_prog.maliarenko.udf.CodesConverter;
import com.ucu.adv_prog.maliarenko.udf.PeriodDetection;
import com.ucu.adv_prog.maliarenko.udf.TeamDetection;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
public class BusinessLogic {


    @ShowDataFrameInTheBeginning
    @ShowDataFrameInTheEnd
    public DataFrame doEnrichment(DataFrame dataFrame){

        dataFrame=dataFrame
                .withColumn("code_description", callUDF(CodesConverter.class.getName(),col("code")))
                .withColumn("fromTeam", callUDF(TeamDetection.class.getName(),col("from")))
                .withColumn("toTeam", callUDF(TeamDetection.class.getName(),col("to")))
                .withColumn("period", callUDF(PeriodDetection.class.getName(),col("eventTime")));

        return dataFrame;

    }

    @ShowDataFrameInTheEnd
    public DataFrame doValidation(DataFrame dataFrame){

        Column valid = when(col("code_description").isNull() , false)
                .when(col("period").isNull() , false)
                .otherwise(true);;

        dataFrame=dataFrame.withColumn("valid", valid);


        return dataFrame;

    }

}
