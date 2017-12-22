package com.ucu.adv_prog.maliarenko;

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
        System.out.println( "Hello World!" );
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConf.class);

        BusinessLogic businessLogic = context.getBean(BusinessLogic.class);
        businessLogic.doEnrichment();

    }
}
