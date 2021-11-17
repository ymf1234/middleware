package cn.yumengfei.exchange.direct;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ReceiveLogsDirect02 {

    public static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();

        // 声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 声明一个队列
        channel.queueDeclare("disk", false, false, false, null);

        channel.queueBind("disk", EXCHANGE_NAME, "error");

        // 接收消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("ReceiveLogsDirect02控制台打印接收到的消息"+message);
        };

        channel.basicConsume("disk",true,  deliverCallback, consumerTag -> {});

    }
}
