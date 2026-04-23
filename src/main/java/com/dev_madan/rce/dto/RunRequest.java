package com.dev_madan.rce.dto;

import lombok.Data;

@Data  // generates getters, setters, equals, hashCode, toString
public class RunRequest {
    private String sessionId;
    private String lang;  // "python" | "java" | "cpp"
    private String code;

}
