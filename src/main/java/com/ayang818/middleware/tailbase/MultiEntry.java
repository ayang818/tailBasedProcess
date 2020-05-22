package com.ayang818.middleware.tailbase;

import com.ayang818.middleware.tailbase.backend.WSStarter;
import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MultiEntry {

    private static final Logger logger = LoggerFactory.getLogger(MultiEntry.class);

    public static void main(String[] args) {
        if (BaseUtils.isBackendProcess()) {
            // BackendController.init();
            // start websocket service
            WSStarter wsStarter = new WSStarter();
            wsStarter.run();
        }
        if (BaseUtils.isClientProcess()) {
            ClientDataStreamHandler.init();
        }
        String port = System.getProperty("server.port", "8080");
        SpringApplication.run(MultiEntry.class,
                "--server.port=" + port
        );
    }

}
