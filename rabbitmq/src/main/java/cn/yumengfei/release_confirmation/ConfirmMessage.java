package cn.yumengfei.release_confirmation;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

/**
 * 发布确认模式
 *      1.单个确认模式
 *      2.批量确认模式
 *      3.异步批量确认
 */
public class ConfirmMessage {
    protected static final int MESSAGE_COUNT = 1000;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        // 单个确认
//        ConfirmMessage.publishMessageIndividually();
        // 批量确认
//        ConfirmMessage.publishMessageBatch();

        // 异步确认
        ConfirmMessage.publishMessageAsync();
    }

    // 单个确认
    public static void publishMessageIndividually() throws IOException, TimeoutException, InterruptedException {
        Channel channel = RabbitMqUtils.getChannel();

        // 队列的声明
        String queueNmae = UUID.randomUUID().toString();
        channel.queueDeclare(queueNmae, true, false, false, null);

        // 开启发布确认
        channel.confirmSelect();

        // 开始时间
        long begin = System.currentTimeMillis();

        // 批量发消息
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("", queueNmae, null, message.getBytes(StandardCharsets.UTF_8));
            // 单个消息就马上进行发布确认
            boolean flag = channel.waitForConfirms();

            if (flag) {
                System.out.println("消息发送成功");
            }

        }

        // 结束时间
        long end = System.currentTimeMillis();

        System.out.println("发布" + MESSAGE_COUNT + "个单独确认消息，耗时" + (end - begin) + "ms");
    }

    // 批量确认
    public static void publishMessageBatch() throws IOException, TimeoutException, InterruptedException {
        Channel channel = RabbitMqUtils.getChannel();

        // 队列的声明
        String queueNmae = UUID.randomUUID().toString();
        channel.queueDeclare(queueNmae, true, false, false, null);

        // 开启发布确认
        channel.confirmSelect();

        // 开始时间
        long begin = System.currentTimeMillis();

        // 批量确认消息大小
        int batchSize = 100;

        // 批量发送消息 批量发布确认
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("", queueNmae, true, null, message.getBytes(StandardCharsets.UTF_8));

            if (i % batchSize == 0) {
                channel.waitForConfirms(); // 发布确认
            }

        }
        channel.waitForConfirms();
        long end = System.currentTimeMillis();

        System.out.println("发布" + MESSAGE_COUNT + "个批量确认消息，耗时" + (end - begin) + "ms");
    }

    // 异步确认
    public static void publishMessageAsync() throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();

        // 队列的声明
        String queueNmae = UUID.randomUUID().toString();
        channel.queueDeclare(queueNmae, true, false, false, null);

        // 开启发布确认
        channel.confirmSelect();



        // 开始时间
        long begin = System.currentTimeMillis();

        /**
         * 线程安全有序的一个哈希表，适用于高并发的情况
         * 1.轻松的将序号与消息进行关联
         * 2.轻松批量删除条目 只要给到序列号
         * 3.支持并发访问
         */
        ConcurrentSkipListMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

        // 准备消息的监听器 监听哪些消息成功了 哪些消息失败了

        // 消息确认成功 回调函数
        ConfirmCallback ackCallback = (deliveryTag, multiple) -> {
            if (multiple) {
                ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(deliveryTag);
                confirmed.clear();
            } else {
                outstandingConfirms.remove(deliveryTag);
            }

            System.out.println("确认的消息" + deliveryTag);

        };


        // 消息确认失败 回调函数
        ConfirmCallback nackCallback = (deliveryTag, multiple) -> {
            outstandingConfirms.get(deliveryTag);
            System.out.println("未确认的消息" + deliveryTag);
        };

        channel.addConfirmListener(ackCallback, nackCallback);

        // 批量发送消息 异步发布确认
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("", queueNmae, true, null, message.getBytes(StandardCharsets.UTF_8));
            outstandingConfirms.put(channel.getNextPublishSeqNo(), message);
        }


        // 结束时间
        long end = System.currentTimeMillis();

        System.out.println("发布" + MESSAGE_COUNT + "个异步确认消息，耗时" + (end - begin) + "ms");

    }
}
