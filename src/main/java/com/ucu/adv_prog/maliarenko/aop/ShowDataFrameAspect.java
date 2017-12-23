package com.ucu.adv_prog.maliarenko.aop;

import org.apache.spark.sql.DataFrame;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Aspect
@Profile("dev")
public class ShowDataFrameAspect {
    @Before("@annotation(ShowDataFrameInTheBeginning)")
    public void showDataFrameInTheBeginnigOfMethod(JoinPoint jp) {
        DataFrame dataFrame = (DataFrame) jp.getArgs()[0];
        System.out.println("ShowDataFrameInTheBeginning aspect is starting:");
        printToConsole(jp, dataFrame);
        System.out.println("ShowDataFrameInTheBeginning aspect. the end");
    }

    @AfterReturning(value = "@annotation(ShowDataFrameInTheEnd)", returning = "dataFrame")
    public void showDataFrameInTheEndOfMethod(JoinPoint jp, DataFrame dataFrame) {
        System.out.println("ShowDataFrameInTheEnd aspect is starting:");
        printToConsole(jp, dataFrame);
        System.out.println("ShowDataFrameInTheEnd aspect. the end");
    }

    private void printToConsole(JoinPoint jp, DataFrame dataFrame) {
        String classname = jp.getTarget().getClass().getSimpleName();
        String methodName = jp.getSignature().getName();
        System.out.println("*****BEGIN*****  " + classname + "  " + methodName + "  *****");
        dataFrame.show();
        System.out.println("*****END*******  " + classname + "  " + methodName + "  *****");

    }


}
