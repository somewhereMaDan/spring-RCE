package com.dev_madan.rce.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
@RestController
@RequestMapping("/me")
public class hello {
  @GetMapping()
  public ResponseEntity<?> checkBackend() {
    return ResponseEntity.ok("ok from spring backend");
  }
}
