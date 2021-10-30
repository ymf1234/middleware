package cn.yumengfei.release_confirmation;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 发布确认模式
 *      1.单个确认模式
 *      2.批量确认模式
 *      3.异步批量确认
 */
public class ConfirmMessage {
    protected static final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();
        //开启发布确认
        channel.confirmSelect();
        //声明队列
        boolean durable = true; // 需要让队列进行持久化
        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);

        // 控制台中输入信息
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String message = scanner.next();
            // 设置生产者发送消息为持久化消息（要去保存到磁盘中） 保存在内存中
            channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("生产者发出消息：" + message);

        }
    }

    // 单个确认
    public static void publishMessage() {

    }
}
