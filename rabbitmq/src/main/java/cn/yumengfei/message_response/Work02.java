package cn.yumengfei.message_response;

import cn.yumengfei.utils.RabbitMqUtils;
import cn.yumengfei.utils.SleepUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 手动应答
 * 消费者1
 */
public class Work02 {
    private static final String ACK_QUEUE_NAME="ack_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();
        int prefetchCount = 1;
        channel.basicQos(prefetchCount); // 配置不公平分发
        System.out.println("C1 等待接收消息处理时间较短");

        //消息消费的时候如何处理消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody());
            SleepUtils.sleep(1);
            System.out.println("接收到信息："+ message);
            /**
             * 1.消息标记 tag
             * 2.是否批量应答未应答消息
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // 手动应答
        boolean autoAck = false;
        channel.basicConsume(ACK_QUEUE_NAME, autoAck, deliverCallback, (consumerTag)->{
            System.out.println(consumerTag+"消费者取消消费接口回调逻辑");
        });


    }
}
