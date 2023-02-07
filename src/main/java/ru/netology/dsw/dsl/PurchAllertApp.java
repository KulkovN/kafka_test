package ru.netology.dsw.dsl;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/*Вариант приложения, который запишет в топик сообщение. каждый раз, когда сумма,
* заработанная по конкретному продукту превышает 3 000 за последнюю минуту*/
public class PurchAllertApp {
    public static final String PURCHASE_TOPIC_NAME = "purchases";
    // public static final String RESULT_TOPIC = "product_quantity_alerts-dsl";
    public static final String RESULT_TOPIC = "max_sum_alerts-dsl"; // изменил название рез-его топика для сообщения
    // private static final long MAX_PURCHASES_PER_MINUTE = 10L;
    private static  final long MAX_SUM_QUANT_PER_MIN = 3000L; // ставим лимит суммы, аналогично примеру с урока

    public static void main(String[] args) throws InterruptedException {
        var client = new CachedSchemaRegistryClient("http://localhost:8090", 16);
        var serDeProps = Map.of(
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );

        Topology topology = buildTopology(client, serDeProps);
        System.out.println(topology.describe());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "QuantityAlertsAppDSL");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);
        var purchasesStream = builder.stream(
                PURCHASE_TOPIC_NAME, // указываем имя топика
                Consumed.with(new Serdes.StringSerde(), avroSerde) // указываем тип ключа и тип значения в топике
        );

        Duration oneMinute = Duration.ofMinutes(1);
        purchasesStream.groupBy((key, val) -> val.get("productid").toString(), Grouped.with(new Serdes.StringSerde(), avroSerde)) // сначало заменил правило группировки по сумме,
                // но, т.к. нам нужно анализировать сумму по каждому продукту - вернул к первоначальному значению
                .windowedBy(
                        TimeWindows.of(oneMinute)
                                .advanceBy(oneMinute))
                .aggregate(
                        () -> 0L,
                        (key, val, agg) -> agg += (Long) val.get("quantity") * (Long) val.get("price"), // тут прописываем условие суммы по каждому продукту, как указывает условие задачи
                        Materialized.with(new Serdes.StringSerde(), new Serdes.LongSerde())
                )
                .filter((key, val) -> val > MAX_SUM_QUANT_PER_MIN) // тут меняем границу
                .toStream()
                .map((key, val) -> {
                    Schema schema = SchemaBuilder.record("MaxSumProduct").fields() // меняю имя аллерта
                            .name("window_start")
                            .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                            .noDefault()
                            .requiredLong("number_of_purchases")
                            .endRecord();
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("window_start", key.window().start());
                    record.put("number_of_purchases", val);
                    return KeyValue.pair(key.key(), record);
                })
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }
}
