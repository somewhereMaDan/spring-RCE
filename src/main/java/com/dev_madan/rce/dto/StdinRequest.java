package com.dev_madan.rce.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class StdinRequest {
    private String sessionId;
    private String data;
}
