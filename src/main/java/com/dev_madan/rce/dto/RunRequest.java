package com.dev_madan.rce.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

// generates getters, setters, equals, hashCode, toString
public class RunRequest {
    private String jobId;

    @NotBlank
    @Pattern(regexp = "python|java|cpp")
    private String lang; // "python" | "java" | "cpp"

    @NotBlank
    @Size(max = 65_536)
    private String code;

    @Size(max = 1_048_576)
    private String inputString;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getInputString() {
        return inputString;
    }

    public void setInputString(String inputString) {
        this.inputString = inputString;
    }
}
