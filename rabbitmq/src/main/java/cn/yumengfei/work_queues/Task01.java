package cn.yumengfei.work_queues;

import cn.yumengfei.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 工作队列(又称任务队列)的主要思想是避免立即执行资源密集型任务，而不得不等待它完成。
 * 相反我们安排任务在之后执行。我们把任务封装为消息并将其发送到队列。在后台运行的工作进
 * 程将弹出任务并最终执行作业。当有多个工作线程时，这些工作线程将一起处理这些任务。
 */
public class Task01 {
    private static final String QUEUE_NAME="hello";
    public static void main(String[] args) throws IOException, TimeoutException {
        try(Channel channel = RabbitMqUtils.getChannel();) {
            channel.queueDeclare(QUEUE_NAME, false, false, false,null);
            // 从控制台当中接受信息
            Scanner scanner = new Scanner(System.in);

            while (scanner.hasNext()) {
                String message = scanner.next();
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println("发送消息完成："+message);
            }
        }
    }
}
