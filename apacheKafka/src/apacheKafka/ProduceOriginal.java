package apacheKafka;


import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.util.StringTokenizer;

public class Produce {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//Random rand = new Random();
		
		//put in the necessary properties to create the Kafka connection
		Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 //Create a new Kafka producer
		 Producer<String, String> producer = new KafkaProducer<>(props);
		 
		/*
		 * for(int i = 20; i < 30; i++){
		 * 
		 * //Create a random number //int randomnumberone = rand.nextInt(50) + 1; int
		 * randomnumberone = i; //produce that random number to the test topic
		 * producer.send(new ProducerRecord<String, String>("test", "btn1", "1," +
		 * Integer.toString(randomnumberone)));
		 * 
		 * }
		 */
		 
		 String fileName = "/home/supriya/Documents/Supriya/UMBC_IS/UMBC_Courses/IS_700_IndStudy/SpamFiltering/spam_test.csv";
		 
		 String topic = "kafka_stream";
		 String msg = new String();
	     File f = new File(fileName);
	     FileReader fr = new FileReader(f);
	     BufferedReader reader = new BufferedReader(fr);
	     String line = reader.readLine();
	     
	     
         while (line != null) 
         {
			// String msg = "test message2 !! As a valued network customer you have been selected to receivea 900 prize reward! To claim call 09061701461. Claim code KL341. Valid 12 hours only.";
        	 msg = line.substring(line.indexOf(",")+1);
        	// System.out.println(msg);
			 ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
			 
			 producer.send(record);
			 
			 line = reader.readLine();
			
         }
		 producer.close();
			
	}
}