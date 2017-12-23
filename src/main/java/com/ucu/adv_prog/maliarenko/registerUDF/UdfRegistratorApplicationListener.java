package com.ucu.adv_prog.maliarenko.registerUDF;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


@Component
public class UdfRegistratorApplicationListener implements ApplicationListener<ContextRefreshedEvent> {
    @Autowired
    private ApplicationContext context;

    @Autowired
    private SQLContext sqlContext;

    private Map<Class,DataType> convTypes = new HashMap<>();

    public UdfRegistratorApplicationListener() {
        this.convTypes.put(String.class, DataTypes.StringType);
        this.convTypes.put(Boolean.class, DataTypes.BooleanType);
        //todo another
    }
    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        Collection<Object> udfObjects = context.getBeansWithAnnotation(RegisterUDF.class).values();
        for (Object udfObject : udfObjects) {
            DataType dataType = getDataType(udfObject.getClass().getGenericInterfaces()[0]);
            sqlContext.udf().register(udfObject.getClass().getName(),
                    (UDF1<?, ?>) udfObject,
                    dataType);
        }

        Collection<Object> udf3Objects = context.getBeansWithAnnotation(RegisterUDF3.class).values();
        for (Object udf3Object : udf3Objects) {
            DataType dataType = getDataType(udf3Object.getClass().getGenericInterfaces()[0]);
            sqlContext.udf().register(udf3Object.getClass().getName(),
                    (UDF3<?, ?, ?, ?>) udf3Object,
                    dataType);
        }

    }

    private DataType getDataType(Type t) {
        Class expectedParamClazz = String.class;

        if (t instanceof ParameterizedType) {
            ParameterizedType type = (ParameterizedType) t;
            int numOfArgs  = type.getActualTypeArguments().length;
            expectedParamClazz = (Class) type.getActualTypeArguments()[numOfArgs - 1];
        }

        return convTypes.get(expectedParamClazz);
    }
}
