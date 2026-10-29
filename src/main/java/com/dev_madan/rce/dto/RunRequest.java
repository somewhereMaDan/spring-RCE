package com.dev_madan.rce.dto;

// generates getters, setters, equals, hashCode, toString
public class RunRequest {
    private String jobId;
    private String lang; // "python" | "java" | "cpp"
    private String code;
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
