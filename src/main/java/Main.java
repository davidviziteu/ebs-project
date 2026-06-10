import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

class Stats
{
    static float publications_number;
    static float latency;
    static float match_number;
}
public class Main {
    private static final int BROKER_COUNT = 10;
    private static final int NOTIFIER_COUNT = 3;
    private static final int SUBSCRIPTION_COUNT = 3;

    private static void cleanupStateFiles() {
        Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
        try (Stream<Path> files = Files.list(tmpDir)) {
            files.filter(path -> {
                        String name = path.getFileName().toString();
                        return name.startsWith("ebs-")
                                && (name.endsWith("-state.json") || name.endsWith("-dedup.json"));
                    })
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ex) {
                            System.out.println("Failed deleting state file " + path + ": " + ex.getMessage());
                        }
                    });
        } catch (Exception ex) {
            System.out.println("Failed state cleanup scan: " + ex.getMessage());
        }

        Path stateDir = tmpDir.resolve("ebs-state");
        if (Files.exists(stateDir)) {
            try (Stream<Path> walk = Files.walk(stateDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (Exception ex) {
                                System.out.println("Failed deleting state path " + path + ": " + ex.getMessage());
                            }
                        });
            } catch (Exception ex) {
                System.out.println("Failed ebs-state cleanup: " + ex.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        try {
            TopologyBuilder builder = new TopologyBuilder();
            PublisherSpout publisher = new PublisherSpout(args[0]);

            builder.setSpout("publisher_spout", publisher);

            for (int i = 1; i <= SUBSCRIPTION_COUNT; ++i) {
                builder.setSpout("subscription" + i, new SubscriptionSpout(args[0]));
            }
            for (int i = 1; i <= BROKER_COUNT; ++i) {
                BoltDeclarer brokerDeclarer = builder
                        .setBolt("broker" + i, new BrokerBolt())
                        .fieldsGrouping("publisher_spout", new Fields("target_broker"))
                        .shuffleGrouping("subscription1", "broker" + i)
                        .shuffleGrouping("subscription2", "broker" + i)
                        .shuffleGrouping("subscription3", "broker" + i)
                        .fieldsGrouping("notifier1", "ack_stream", new Fields("owner_broker_id"))
                        .fieldsGrouping("notifier2", "ack_stream", new Fields("owner_broker_id"))
                        .fieldsGrouping("notifier3", "ack_stream", new Fields("owner_broker_id"));

                for (int j = 1; j <= BROKER_COUNT; j++) {
                    if (i == j) {
                        continue;
                    }
                    brokerDeclarer
                            .fieldsGrouping("broker" + j, "replication_stream", new Fields("target_broker"))
                            .fieldsGrouping("broker" + j, "heartbeat_stream", new Fields("target_broker"));
                }
            }

            for (int i = 1; i <= NOTIFIER_COUNT; i++) {
                BoltDeclarer notifierDeclarer = builder.setBolt("notifier" + i, new NotifierBolt());
                for (int b = 1; b <= BROKER_COUNT; b++) {
                    notifierDeclarer.shuffleGrouping("broker" + b, "notifier" + i);
                }
            }


            Config config = new Config();

            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();

            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
            config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);

            cluster.submitTopology("publish_subscribe_topology", config, topology);

            try {
                Thread.sleep(180 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            cluster.killTopology("publish_subscribe_topology");
            cluster.shutdown();
            cluster.close();
            cleanupStateFiles();
            
            if (Stats.publications_number > 0) {
                Stats.latency /= Stats.publications_number;
                Stats.match_number /= Stats.publications_number;
            }
            System.out.println("Number of publications: " + Stats.publications_number);
            System.out.println("Avg latency(milli): " + Stats.latency);
            System.out.println("Avg match rate: " + Stats.match_number);
            System.exit(0);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
