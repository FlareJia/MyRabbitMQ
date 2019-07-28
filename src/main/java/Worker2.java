import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
/* 将35行，51行取消注释 和 修改56行为false 就会公平分发  否则为轮询分发 */
public class Worker2 {
    private final static String QUEUE_NAME = "task_queue";

    public static void doWork(String task){
        for(char ch:task.toCharArray()){
            if(ch == '.'){
                try {
                    Thread.sleep(10);

                }
                catch (InterruptedException _ignored){
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection =factory.newConnection();
        Channel channel = connection.createChannel();

        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        System.out.println(" [*] Waitting for messages. To exit press Ctrl+C");

        // 同一时刻服务器只会发一条消息给消费者
        //channel.basicQos(1);

        // 这一句其实是实现了DeliverCallback的一个接口handle
        DeliverCallback deliverCallback = (consumerTag, delivery) ->{
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message +"'");
            try {
                doWork(message);
                Thread.sleep(10);
            }
            catch (InterruptedException e){
                System.out.println(e);
            }
            finally {
                System.out.println("[x] done");
                //下面这行注释掉表示使用自动确认模式
                //channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }

        };
        boolean autoACK = true;
        channel.basicConsume(QUEUE_NAME, autoACK, deliverCallback, consumerTag -> { });
    }


}
