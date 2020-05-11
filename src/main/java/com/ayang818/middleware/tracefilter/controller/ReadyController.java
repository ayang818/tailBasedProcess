package com.ayang818.middleware.tracefilter.controller;

import com.ayang818.middleware.tracefilter.pojo.PortParamter;
import com.ayang818.middleware.tracefilter.service.DataPuller;
import com.ayang818.middleware.tracefilter.utils.CastUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 9:51
 **/
@RestController
public class ReadyController {

    @Autowired
    private DataPuller dataPuller;

    private static final Logger logger = LoggerFactory.getLogger(ReadyController.class);

    @RequestMapping(value = "ready", method = RequestMethod.HEAD)
    public String ready(HttpServletResponse response) {
        response.setHeader("status", "200");
        return "";
    }

    @RequestMapping(value = "setParamter", method = RequestMethod.POST)
    public String setParamter(@RequestBody PortParamter port, HttpServletResponse response) {
        String dataport = null;
        if (port != null) {
            dataport = port.getDataport();
            // 开始从数据流中拉取数据
            dataPuller.pulldata(dataport);
        } else {
            logger.warn("未接收到数据源端口");
        }
        response.setHeader("status", "200");
        return "";
    }

    @RequestMapping(value = "api/traceData1", method = RequestMethod.GET)
    public void pullTest(HttpServletResponse response) {
        try {
            OutputStream outputStream = response.getOutputStream();
            BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("D:/middlewaredata/nio_data.txt"), StandardCharsets.UTF_8);
            char[] charBuffer = new char[1024];
            byte[] byteBuffer = new byte[1024];
            while (bufferedReader.read(charBuffer) != -1) {
                byte[] bytes = CastUtil.chars2bytes(charBuffer, byteBuffer);
                outputStream.write(bytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
