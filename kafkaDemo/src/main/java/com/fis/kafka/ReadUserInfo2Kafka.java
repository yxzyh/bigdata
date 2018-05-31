package com.fis.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import net.sf.json.JSONObject;


import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Geoge
 * @date 2018/5/31 0031 上午 11:01
 */
public class ReadUserInfo2Kafka {

    public static String confPath = System.getProperty("user.dir") + File.separator + "conf";

    public static void main(String[] args) {
//        if(args.length < 1){
//            System.out.println("缺少输出的参数，请指定要处理的text文本");
//            System.exit(1);
//        }

      //  URL url = ReadUserInfo2Kafka.class.getResource("/app.properties");
        InputStream inputStream = ReadUserInfo2Kafka.class.getResourceAsStream("/app.properties");
        System.out.println(File.separator);
        System.out.println(inputStream);
        String filePath = args[0];
        BufferedReader reader = null;

        try {

            Properties appProperties = new Properties();
            appProperties.load(inputStream);
            String brokerlist = String.valueOf(appProperties.get("bootstrap.servers"));
            String topic_name = String.valueOf(appProperties.get("topic.name"));

            Properties props = getKafkaProps();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerlist);
            Producer<String,String> producer = new KafkaProducer<String, String>(props);

            reader = new BufferedReader(new FileReader(filePath));
            String tempString = null;
            int line = 1;
            //一次读入一行，直到结束为止。
            while((tempString = reader.readLine()) != null){
                String detailJson = parse_person_info_JSON(tempString);
                ProducerRecord record = new ProducerRecord<String,String>(topic_name,detailJson);
                producer.send(record);
                line++;
            }
            reader.close();
            producer.flush();
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

    }

    private static String parse_person_info_JSON(String tempString){
        if(tempString != null && tempString.length() > 0){
            Map<String,String> resultMap = null;
            String [] detail = tempString.split("\001");
            resultMap = new HashMap<String,String>();
            resultMap.put("id",detail[0]);
            resultMap.put("name",detail[1]);
            resultMap.put("sex",detail[2]);
            resultMap.put("city",detail[3]);
            resultMap.put("occupation",detail[4]);
            resultMap.put("mobile_phone_num",detail[5]);
            resultMap.put("fix_phone_num",detail[6]);
            resultMap.put("bank_name",detail[7]);
            resultMap.put("address",detail[8]);
            resultMap.put("marriage",detail[9]);
            resultMap.put("chid_num",detail[10]);
            return net.sf.json.JSONObject.fromObject(resultMap).toString();

        }
        return  null;
    }

    private static Properties getKafkaProps() {

        try {
            Properties prop = new Properties();
            prop.put(ProducerConfig.ACKS_CONFIG, "all");
            prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);//设置批量发送信息
            prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            return prop;
        }catch(Exception e){
            e.printStackTrace();
        }

        return  null;


    }
}


