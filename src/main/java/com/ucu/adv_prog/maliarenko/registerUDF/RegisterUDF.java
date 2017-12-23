package com.ucu.adv_prog.maliarenko.registerUDF;


import org.springframework.stereotype.Component;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Component
public @interface RegisterUDF {
}
