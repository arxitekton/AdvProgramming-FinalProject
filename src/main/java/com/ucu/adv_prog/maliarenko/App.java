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
        //String test = "code=10;from=Jonathan Tah;to=Oleg Shatov;eventTime=200:38;stadion=room;startTime=13:18;";
        //Map<String, String> map = Splitter.on( ";" ).omitEmptyStrings().trimResults().withKeyValueSeparator( '=' ).split( test );
        //System.out.println(map);

    }
}
