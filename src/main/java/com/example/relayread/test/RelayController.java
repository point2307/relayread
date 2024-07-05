package com.example.relayread.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@org.springframework.stereotype.Controller
public class RelayController {

    @Autowired
    private RelayService relayLogReaderService;

    @GetMapping("/start")
    public String startReadingRelayLog(@RequestParam String relayLogDirectory) {
        relayLogReaderService.startReadingRelayLog("/arch_log/marialog/relay/");
        return "Relay log reading started.";
    }

}
