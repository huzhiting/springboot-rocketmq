package com.fzph.producer.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fzph.demo.entity.MQEntity;
import com.fzph.demo.util.SerializableUtil;
import com.fzph.producer.service.IProducer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.UUID;

@Service
public class ProducerServiceImpl implements IProducer {

    private static Logger logger = LoggerFactory.getLogger(ProducerServiceImpl.class);

    @Value("${spring.rocketmq.name-server}")
    private String namesrvAddr;

    @Value("${spring.rocketmq.producer.group}")
    private String producerGroup;

    private DefaultMQProducer producer;

    /**
     * spring 容器初始化所有属性后调用此方法
     */
    @PostConstruct
    public void defaultMQProducer() throws Exception {
        producer = new DefaultMQProducer();
        producer.setProducerGroup( this.producerGroup );
        producer.setNamesrvAddr( this.namesrvAddr );
//        producer.setVipChannelEnabled(false);
        /*
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();
        logger.info( "[{}:{}] start successd!",producerGroup,namesrvAddr );
    }

    public Message message(String topic, MQEntity entity) {
        String keys = UUID.randomUUID().toString();
        entity.setMqKey(keys);
        String tags = entity.getClass().getName();
        //  logger.info("业务:{},tags:{},keys:{},entity:{}",topic, tags, keys, entity);
        String smsContent = MessageFormat.format("业务:{0},tags:{1},keys:{2},entity:{3}",topic,tags,keys, JSONObject.toJSONString(entity));
        logger.info(smsContent);

        Message msg = null;
        try {
            msg = new Message(topic, tags, keys,
                    JSON.toJSONString(entity).getBytes(RemotingHelper.DEFAULT_CHARSET));
        } catch (UnsupportedEncodingException e) {
            logger.error("消息转码失败",e);
        }
        return msg;
    }

    @Override
    public void send(String topic, MQEntity entity) {
        Message msg = message(topic,entity);
        try {
            producer.send(msg);
        } catch (Exception e) {
            logger.error(entity.getMqKey().concat(":发送消息失败"), e);
            throw new RuntimeException("发送消息失败",e);
        }

    }

    @Override
    public void send(String topic, MQEntity entity, SendCallback sendCallback) {
        Message msg = message(topic,entity);
        try {
            producer.send(msg, sendCallback);
        } catch (Exception e) {
            logger.error(entity.getMqKey().concat(":发送消息失败"), e);
            throw new RuntimeException("发送消息失败",e);
        }
    }

    @Override
    public void sendOneway(String topic, MQEntity entity) {
        Message msg = message(topic,entity);
        try {
            producer.sendOneway(msg);
        } catch (Exception e) {
            logger.error(entity.getMqKey().concat(":发送消息失败"), e);
            throw new RuntimeException("发送消息失败",e);
        }

    }
}
