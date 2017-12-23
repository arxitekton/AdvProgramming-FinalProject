package com.ucu.adv_prog.maliarenko.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MatchEvent {
    private String code;
    private String from;
    private String to;
    private String eventTime;
    private String stadion;
}
