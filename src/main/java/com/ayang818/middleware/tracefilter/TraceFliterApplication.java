package com.ayang818.middleware.tracefilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TraceFliterApplication {

    private static final Logger logger = LoggerFactory.getLogger(TraceFliterApplication.class);

    public static void main(String[] args) {
        String envPort = System.getenv("SERVER_PORT");
        String port = null;
        if (envPort == null) {
            port = System.getProperty("server.port");
        } else {
            port = envPort;
        }
        logger.info("数据过滤应用启动在 {} 端口", port);
        SpringApplication.run(TraceFliterApplication.class, "--server.port=" + port);
    }

}
