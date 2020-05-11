package com.ayang818.middleware.tracefilter.service.impl;

import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import com.ayang818.middleware.tracefilter.service.DataPuller;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ayang818.middleware.tracefilter.utils.HttpUtil.getHttpClient;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 10:16
 **/
@Service
public class DataPullerImpl implements DataPuller {

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
        Future<Response> response = httpClient.prepareHead(dataSourceUrl).execute();
        try {
            InputStream dataStream = response.get().getResponseBodyAsStream();
            // 开始处理数据读入流
            dataStreamHandler.handleDataStream(dataStream);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
