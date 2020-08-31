package com.ebei.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.ebei.canal.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.net.InetSocketAddress;

@EnableTransactionManagement
@SpringBootApplication
public class CanalClient {
    protected final static Logger logger = LoggerFactory.getLogger(CanalClient.class);
    
    private static String ADDRESS = ConfigUtils.getConfigValue("application.properties", "canal.address");
    
    private static int PORT = Integer.parseInt(ConfigUtils.getConfigValue("application.properties", "canal.port"));
    
    private static String DESTINATION = ConfigUtils.getConfigValue("application.properties", "canal.destination");
    
    private static String USERNAME = ConfigUtils.getConfigValue("application.properties", "canal.username");
    
    private static String PASSWORD = ConfigUtils.getConfigValue("application.properties", "canal.password");

    private static String SUBSCRIBER = ConfigUtils.getConfigValue("application.properties", "canal.subscriber");

    public static void main(String args[]) {
        SpringApplication app = new SpringApplication(CanalClient.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
        // 创建链接
        logger.info("Trying to connect to " + ADDRESS + ":" + PORT);
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ADDRESS,
                PORT), DESTINATION, USERNAME, PASSWORD);
        int batchSize = 1000;
        try {
            logger.info("...");
            connector.connect();
            logger.info("connected");
            connector.subscribe(SUBSCRIBER);
            connector.rollback();
    
            logger.info("CanalClient Application started successfully!");
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    DataProcessor.process(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Canal Client exit with error.");
            System.exit(-2);
        } finally {
            connector.disconnect();
        }
    }

}