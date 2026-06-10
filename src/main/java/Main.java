import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

class Stats
{
    static float publications_number;
    static float latency;
    static float match_number;
}
public class Main {
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
    }

    public static void main(String[] args) {
        try {
            TopologyBuilder builder = new TopologyBuilder();
            PublisherSpout publisher = new PublisherSpout(args[0]);

            builder.setSpout("publisher_spout", publisher);

            for (int i = 1; i <= 3; ++i) {
                builder.setSpout("subscription" + i, new SubscriptionSpout(args[0]));
            }
            for (int i = 1; i <= 3; ++i) {
                BoltDeclarer brokerDeclarer = builder
                        .setBolt("broker" + i, new BrokerBolt())
                        .allGrouping("publisher_spout")
                        .shuffleGrouping("subscription1", "broker" + i)
                        .shuffleGrouping("subscription2", "broker" + i)
                        .shuffleGrouping("subscription3", "broker" + i)
                        .shuffleGrouping("notifier1", "ack_stream")
                        .shuffleGrouping("notifier2", "ack_stream")
                        .shuffleGrouping("notifier3", "ack_stream");

                for (int j = 1; j <= 3; j++) {
                    if (i == j) {
                        continue;
                    }
                    brokerDeclarer
                            .shuffleGrouping("broker" + j, "replication_stream")
                            .shuffleGrouping("broker" + j, "heartbeat_stream");
                }
            }

            for (int i = 1; i <= 3; i++) {
                builder.setBolt("notifier" + i, new NotifierBolt()).shuffleGrouping("broker1", "notifier" + i).shuffleGrouping("broker2", "notifier" + i).shuffleGrouping("broker3",  "notifier" + i);
            }


            Config config = new Config();

            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();

            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
            config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);

            cluster.submitTopology("publish_subscribe_topology", config, topology);

            try {
                Thread.sleep(100 * 1000);
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
