package com.ucu.adv_prog.maliarenko.config;

import lombok.Getter;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


@Component
public class CodesConfig implements Serializable{
    @Getter
    @Resource(name = "codesProperties")
    private Map<Integer, String> map = new HashMap<>();

}
