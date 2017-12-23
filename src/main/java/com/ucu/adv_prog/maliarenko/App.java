package com.ucu.adv_prog.maliarenko;

import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Map;
import com.google.common.base.Splitter;

/**
 * Final Project!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Final Project!" );
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConf.class);
        context.getEnvironment().setActiveProfiles("dev");

        DataFrameBuilder dataFrameBuilder = context.getBean(DataFrameBuilder.class);
        DataFrame dataFrame = dataFrameBuilder.load();

        BusinessLogic businessLogic = context.getBean(BusinessLogic.class);
        DataFrame newDataFrame = businessLogic.doEnrichment(dataFrame);

    }
}
