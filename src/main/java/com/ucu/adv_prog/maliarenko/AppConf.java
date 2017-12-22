package com.ucu.adv_prog.maliarenko;

import com.ucu.adv_prog.maliarenko.bpp.AutowiredBroadcastBPP;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;

@Configuration
@ComponentScan
@PropertySource("classpath:app.properties")
public class AppConf {
    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(new SparkConf().setMaster("local").setAppName("football"));
    }

    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc());
    }

    @Bean
    public AutowiredBroadcastBPP autowiredBroadcastBPP() {
        return new AutowiredBroadcastBPP();
    }

    @Bean(name = "codesProperties")
    public static PropertiesFactoryBean codesMapper() {
        PropertiesFactoryBean bean = new PropertiesFactoryBean();
        bean.setLocation(new ClassPathResource(
                "codes.properties"));
        return bean;
    }

    @Bean(name = "teamsProperties")
    public static PropertiesFactoryBean teamsMapper() {
        PropertiesFactoryBean bean = new PropertiesFactoryBean();
        bean.setLocation(new ClassPathResource(
                "teams.properties"));
        return bean;
    }

}
