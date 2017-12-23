package com.ucu.adv_prog.maliarenko.config;

import lombok.Getter;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


@Component
public class TeamsConfig implements Serializable{
    @Getter
    @Resource(name = "teamsProperties")
    private Map<String, String> map = new HashMap<>();

}
