package com.sqt.rocketmq.producer;

import com.sqt.rocketmq.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Description:
 * @author: sqt(男 ， 未婚) 微信:810548252
 * @Date: Created in 2019-07-13  0:44
 */
@Slf4j
public class Producer {

    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test-producer");
        producer.setNamesrvAddr(Constant.MQ_NAME_ADDR);
        producer.start();
        for(int i = 0;i < 5 ;i++){
            Message message = new Message("test-topic","tags","key"+i,("hello!RocketMQ"+i).getBytes());
            SendResult result = producer.send(message);
            log.info("返回的结果:{}",result);
        }
        producer.shutdown();
    }
}
