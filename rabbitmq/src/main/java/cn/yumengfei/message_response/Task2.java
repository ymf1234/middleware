package cn.yumengfei.message_response;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 消息应答
 *
 * 概念
     * 消费者完成一个任务可能需要一段时间，如果其中一个消费者处理一个长的任务并仅只完成
     * 了部分突然它挂掉了，会发生什么情况。RabbitMQ 一旦向消费者传递了一条消息，便立即将该消
     * 息标记为删除。在这种情况下，突然有个消费者挂掉了，我们将丢失正在处理的消息。以及后续
     * 发送给该消费这的消息，因为它无法接收到。
     * 为了保证消息在发送过程中不丢失，rabbitmq 引入消息应答机制，消息应答就是:消费者在接
     * 收到消息并且处理该消息之后，告诉 rabbitmq 它已经处理了，rabbitmq 可以把该消息删除了。
 *
 * 自动应答
     * 消息发送后立即被认为已经传送成功，这种模式需要在高吞吐量和数据传输安全性方面做权
     * 衡,因为这种模式如果消息在接收到之前，消费者那边出现连接或者 channel 关闭，那么消息就丢
     * 失了,当然另一方面这种模式消费者那边可以传递过载的消息，没有对传递的消息数量进行限制，
     * 当然这样有可能使得消费者这边由于接收太多还来不及处理的消息，导致这些消息的积压，最终
     * 使得内存耗尽，最终这些消费者线程被操作系统杀死，所以这种模式仅适用在消费者可以高效并
     * 以某种速率能够处理这些消息的情况下使用
 *
 * 消息应答的方法
    * A.Channel.basicAck(用于肯定确认)
        * RabbitMQ 已知道该消息并且成功的处理消息，可以将其丢弃了
    * B.Channel.basicNack(用于否定确认)
    * C.Channel.basicReject(用于否定确认) 与 Channel.basicNack 相比少一个参数
        * 不处理该消息了直接拒绝，可以将其丢弃了
 *
 * Multiple 的解释
    * 手动应答的好处是可以批量应答并且减少网络拥堵
    *  multiple 的 true 和 false 代表不同意思
        *  true 代表批量应答 channel 上未应答的消息
        *  false 同上面相比
 *
 *  消息自动重新入队
     *  如果消费者由于某些原因失去连接(其通道已关闭，连接已关闭或 TCP 连接丢失)，导致消息
     * 未发送 ACK 确认，RabbitMQ 将了解到消息未完全处理，并将对其重新排队。如果此时其他消费者
     * 可以处理，它将很快将其重新分发给另一个消费者。这样，即使某个消费者偶尔死亡，也可以确
     * 保不会丢失任何消息。
 */

//生产者

/**
 *
 * 手动应答
 * 持久化
 * 不公平分发
 */
public class Task2 {
    private static final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        // 手动应答
//        Task2.manualAnswer();

        // 持久化
        Task2.persistence();
    }

    /**
     * 手动应答
     */
    private static void manualAnswer() throws IOException, TimeoutException {
        try(Channel channel = RabbitMqUtils.getChannel()) {
            /**
             * 生成一个队列
             * 1.队列名称
             * 2.队列里面的消息是否持久化 默认消息存储在内存中
             * 3.该队列是否只供一个消费者进行消费 是否进行共享 true 可以多个消费者消费
             * 4.是否自动删除 最后一个消费者端开连接以后 该队列是否自动删除 true 自动删除
             * 5.其他参数
             */
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            Scanner sc = new Scanner(System.in);
            System.out.println("请输入信息");
            while (sc.hasNext()) {
                String message = sc.nextLine();
                channel.basicPublish("", TASK_QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("生产者发出信息：" + message);
            }
        }
    }

    /**
     * 持久化、不公平分发
     * @throws IOException
     * @throws TimeoutException
     */
    private static void persistence() throws IOException, TimeoutException {
        try(Channel channel = RabbitMqUtils.getChannel()) {
            /**
             * 生成一个队列
             * 1.队列名称
             * 2.队列里面的消息是否持久化 默认消息存储在内存中
             * 3.该队列是否只供一个消费者进行消费 是否进行共享 true 可以多个消费者消费
             * 4.是否自动删除 最后一个消费者端开连接以后 该队列是否自动删除 true 自动删除
             * 5.其他参数
             */
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            Scanner sc = new Scanner(System.in);
            System.out.println("请输入信息");
            while (sc.hasNext()) {
                String message = sc.nextLine();
                // 持久化
                channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("生产者发出信息：" + message);
            }
        }
    }
}
