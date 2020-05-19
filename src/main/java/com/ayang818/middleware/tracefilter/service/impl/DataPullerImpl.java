package com.ayang818.middleware.tracefilter.service.impl;

import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import com.ayang818.middleware.tracefilter.service.DataPuller;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 10:16
 **/
@Service
public class DataPullerImpl implements DataPuller {

    private static final Logger logger = LoggerFactory.getLogger(DataPuller.class);

    @Autowired
    DataStreamHandler dataStreamHandler;

    @Override
    public void pulldata(String dataport) {
        String serverPort = System.getProperty("server.port");

        String dataSourceUrl;
        // 根据环境变量中的容器编号，拉取不同源头的数据，数据URL ：http://localhost:port/trace${imageNumber}.data
        if ("8000".equals(serverPort)) {
            logger.info("准备拉取 trace1.data......");
            //dataSourceUrl = "http://localhost:" + dataport + "/trace1.data";
            dataSourceUrl = "http://localhost:8000/trace1.data";
        } else {
            logger.info("准备拉取 trace2.data......");
            //dataSourceUrl = "http://localhost:" + dataport + "/trace2.data";
            dataSourceUrl = "http://localhost:8001/trace2.data";
        }
        CloseableHttpClient client = HttpClients.createDefault();
        try {
            logger.info("开始连接数据源 {}......", dataSourceUrl);
            HttpResponse response = client.execute(new HttpGet(dataSourceUrl));
            dataStreamHandler.handleDataStream(response.getEntity().getContent());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
