package com.dev_madan.rce.dto;

import lombok.Data;

 // generates getters, setters, equals, hashCode, toString
public class RunRequest {
     private String sessionId;
     private String lang;  // "python" | "java" | "cpp"
     private String code;
     private String inputString;
     public String getSessionId() {
         return sessionId;
     }

     public void setSessionId(String sessionId) {
         this.sessionId = sessionId;
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
