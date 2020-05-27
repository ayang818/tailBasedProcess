package com.ayang818.middleware.tailbase;

import com.ayang818.middleware.tailbase.backend.MessageHandler;
import com.ayang818.middleware.tailbase.backend.PullDataService;
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
        String port = System.getProperty("server.port", "8080");
        if (BaseUtils.isBackendProcess()) {
            MessageHandler.init();
            // start websocket service
            new WSStarter().run();
            // start consume thread
            PullDataService.start();
            logger.info("数据后端已在 {} 端口启动......", port);
        }
        if (BaseUtils.isClientProcess()) {
            ClientDataStreamHandler.init();
            logger.info("数据客户端已在 {} 端口启动......", port);
        }
        SpringApplication.run(MultiEntry.class,
                "--server.port=" + port
        );
    }

}
