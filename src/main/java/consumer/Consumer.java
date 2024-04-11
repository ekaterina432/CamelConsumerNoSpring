package consumer;


import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.component.ComponentsBuilderFactory;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws Exception {
        try (CamelContext camelContext = new DefaultCamelContext()){
            camelContext.getPropertiesComponent().setLocation("classpath:application.properties");
            setUpKafkaComponent(camelContext);
            camelContext.addRoutes(createRouteBuilder());
            camelContext.start();
            Thread.sleep(50_000);
            camelContext.stop();

        }
    }

    static void setUpKafkaComponent(CamelContext camelContext) {
        ComponentsBuilderFactory.kafka()
                .brokers("{{kafka.brokers}}")
                //озволяет использовать компонент Kafka в маршрутах Camel для чтения и записи сообщений в Apache Kafka
                .register(camelContext, "kafka");
    }

    static RouteBuilder createRouteBuilder(){
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("kafka:{{consumer.topic}}"
                         + "?maxPollRecords={{consumer.maxPollRecords}}"
                         + "&consumersCount={{consumer.consumersCount}}"
                         + "&seekTo={{consumer.seekTo}}"
                         + "&groupId={{consumer.group}}")
                        //логирует содержимое тела каждого сообщения
                        .routeId("FromKafka")
                        .log("${body}");

            }
        };
    }


}
