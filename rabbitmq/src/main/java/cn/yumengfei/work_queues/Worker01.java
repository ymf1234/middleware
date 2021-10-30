package cn.yumengfei.work_queues;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作队列(又称任务队列)的主要思想是避免立即执行资源密集型任务，而不得不等待它完成。
 * 相反我们安排任务在之后执行。我们把任务封装为消息并将其发送到队列。在后台运行的工作进
 * 程将弹出任务并最终执行作业。当有多个工作线程时，这些工作线程将一起处理这些任务。
 */
public class Worker01 {
    private static final String QUEUE_NAME="hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMqUtils.getChannel();
        DeliverCallback deliverCallback = (consumerTag, delivery)->{
            String receivedMessage = new String(delivery.getBody());
            System.out.println("接收到消息:"+receivedMessage);
        };

        CancelCallback cancelCallback = (consumerTag)->{
            System.out.println(consumerTag+"消费者取消消费接口回调逻辑");
        };

        System.out.println("C2 消费者启动等待消费......");
        channel.basicConsume(QUEUE_NAME,true,deliverCallback,cancelCallback);
    }
}
