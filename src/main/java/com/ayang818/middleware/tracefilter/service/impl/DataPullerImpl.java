package com.ayang818.middleware.tracefilter.service.impl;

import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import com.ayang818.middleware.tracefilter.service.DataPuller;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static com.ayang818.middleware.tracefilter.utils.HttpUtil.getHttpClient;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 10:16
 **/
@Service
public class DataPullerImpl implements DataPuller {

    private static final Logger logger = LoggerFactory.getLogger(DataPuller.class);

    @Value("${env.dataId}")
    String dataId;

    @Autowired
    DataStreamHandler dataStreamHandler;

    @Override
    public void pulldata(String dataport) {
        assert dataId == null : "应用数据源没有正确设置";
        Integer port = Integer.valueOf(dataport);
        AsyncHttpClient httpClient = getHttpClient();
        String dataSourceUrl = "http://localhost:" + port + "/api/traceData" + dataId;
        CloseableHttpClient client = HttpClients.createDefault();
        try {
            logger.info("开始连接数据源......");
            HttpResponse response = client.execute(new HttpGet(dataSourceUrl));
            dataStreamHandler.handleDataStream(response.getEntity().getContent());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
