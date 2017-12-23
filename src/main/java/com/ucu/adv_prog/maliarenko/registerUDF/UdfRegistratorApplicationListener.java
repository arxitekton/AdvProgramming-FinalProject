package com.ucu.adv_prog.maliarenko.registerUDF;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.Collection;


@Component
public class UdfRegistratorApplicationListener implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired
    private ApplicationContext context;

    @Autowired
    private SQLContext sqlContext;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        Collection<Object> udfObjects = context.getBeansWithAnnotation(RegisterUDF.class).values();
        for (Object udfObject : udfObjects) {
            sqlContext.udf().register(udfObject.getClass().getName(),
                    (UDF1<?, ?>) udfObject,
                    DataTypes.StringType);
        }

        Collection<Object> udf3Objects = context.getBeansWithAnnotation(RegisterUDF3.class).values();
        for (Object udf3Object : udf3Objects) {
            sqlContext.udf().register(udf3Object.getClass().getName(),
                    (UDF3<?, ?, ?, ?>) udf3Object,
                    DataTypes.BooleanType);
        }

    }
}
