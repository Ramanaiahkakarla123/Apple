import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;
 
public class KafkaRandomNumberProducer {
 
    public static void main(String args[]) throws Exception{
 
        //properties for producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.160.0.2:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                          
        //create producer
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
                                       
        //send messages to my-topic
        try {
        for(int i = 0; i < 10000; i++) {
		Random rand = new Random();
		Integer randInt = rand.nextInt(100000);
        	ProducerRecord producerRecord = new ProducerRecord<Integer, String>("kafka_random_number", i, randInt.toString() );
                producer.send(producerRecord);
	        Thread.sleep(5000);	
        }
        } finally {                                                                  
        //close producer
        producer.close();
}
}                                                                                                         
}
