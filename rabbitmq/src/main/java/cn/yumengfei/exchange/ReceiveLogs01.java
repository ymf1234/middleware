package cn.yumengfei.exchange;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ReceiveLogs01 {

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
        channel.queueDeclare().getQueue();

        /**
         * 绑定交换机与队列
         */
        channel.queueBind();

    }
}
