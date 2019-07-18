package com.sqt.rocketmq.consumer;

import com.sqt.rocketmq.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @Description:
 * @author: sqt(男 ， 未婚) 微信:810548252
 * @Date: Created in 2019-07-14  2:16
 */
@Slf4j
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer");
        //设置nameserv地址
        consumer.setNamesrvAddr(Constant.MQ_NAME_ADDR);
        //设置从哪开始i消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //注册监听哪个topic,以及tags过滤
        consumer.subscribe("test-topic","tags");
        //注册监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);
                try{
                    String topic = messageExt.getTopic();
                    String tags = messageExt.getTags();
                    String keys = messageExt.getKeys();
                    if (keys.equals("key3")){
                        throw new Exception("消费消息失败");
                    }
                    String body = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    log.info("topic:{},tags:{},keys:{},body:{}",topic,tags,keys,body);
                }catch (Exception e){
                    log.info("出异常了，异常信息:{}",e.getLocalizedMessage());
                    //获取消息重试次数
                    int times = messageExt.getReconsumeTimes();
                    log.info("消息重试次数:{}",times);
                    //当消费三次还没有消费成功时返回成功，
                    if (times == 3){
                        //todo 将消费失败的消息记录下来,人工处理
                        messageExt.getBody();
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    //告诉broker重发
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        log.info("============>consumer开始消费");
    }


}
