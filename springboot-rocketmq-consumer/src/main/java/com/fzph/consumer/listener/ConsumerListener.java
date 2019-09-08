package com.fzph.consumer.listener;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;


@Component
public class ConsumerListener {

    protected Logger logger = LoggerFactory.getLogger(ConsumerListener.class);

    @Value("${spring.rocketmq.name-server}")
    private String namesrvAddr;//NameServer 地址

    @Value("${spring.rocketmq.consumer.group}")
    private String consumerGroup;//消费者的组名

    @Value("${spring.rocketmq.topic}")
    private String consumerTopic;//消费者的主题

    @PostConstruct
    public void defaultMQPushConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        try {
            consumer.subscribe(consumerTopic, "");
            consumer.registerMessageListener((MessageListenerOrderly) (list, consumeOrderlyContext) -> {
                consumeOrderlyContext.setAutoCommit(true);
                for (MessageExt msg : list) {

                    String msgContent = null;
                    try {
                        msgContent = new String(msg.getBody(),"UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        logger.error("转码失败",e);
                    }
                    logger.info("######### MSG Content start ##########");
                    logger.info(JSON.toJSONString(msgContent));
                    logger.info("#########        END        ##########");

                }
                return ConsumeOrderlyStatus.SUCCESS;
            });
            consumer.start();
            logger.info("Consumer started.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
