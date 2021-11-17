package cn.yumengfei.exchange.fanout;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ReceiveLogs02 {

    public static final String EXCHANGE_NAME = "logs";
    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();

        // 声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        // 声明一个临时队列
        /**
         * 生成一个临时队列，队列名称是随机的
         * 当消费者断开与队列的连接的时候 队列就自动删除
         */
        String queueName = channel.queueDeclare().getQueue();

        /**
         * 绑定交换机与队列
         */
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        System.out.println("等待接收消息,把接收到的消息打印在屏幕....");

        // 接收消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("ReceiveLogs02控制台打印接收到的消息"+message);
        };

        // 消费者取消消息

        channel.basicConsume(queueName,true,  deliverCallback, consumerTag -> {});

    }
}
