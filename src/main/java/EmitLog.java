import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.ThreadPoolExecutor;

public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String message = "info: Hello World!";
            for(int i=0; i<100; i++){
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                System.out.println(" [x] Sent '" + message + i +"'");
                Thread.sleep(i*10);
            }

        }
    }

}