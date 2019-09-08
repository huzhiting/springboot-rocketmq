package com.fzph.producer.controller;

import com.fzph.demo.entity.MQEntity;
import com.fzph.producer.service.IProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("/mq")
public class ProducerController {

    @Autowired
    private IProducer iProducer;

    @Value("${spring.rocketmq.topic}")
    private String topic;

    @RequestMapping("/producemsg")
    public void producemsg(){
        MQEntity mqEntity = new MQEntity();
        mqEntity.addExt("createTime",new Date());
        mqEntity.addExt("msg","发送一条消息");
        iProducer.send(topic,mqEntity);
    }
}
