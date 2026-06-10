import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NotifierBolt extends BaseRichBolt {
    private static final long serialVersionUID = 3;
    private String task;
    private OutputCollector collector;
    private final Set<String> seenDeliveries = new HashSet<>();
    private transient Path dedupFile;
    private transient Gson gson;

    // remove template type qualifiers from conf declaration for Storm v1
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.task = context.getThisComponentId();
        this.dedupFile = Paths.get(System.getProperty("java.io.tmpdir"), "ebs-" + task + "-dedup.json");
        this.gson = new Gson();
        loadDedupSet();
        System.out.println("----- Started task: "+this.task);

    }

    private void loadDedupSet() {
        if (!Files.exists(dedupFile)) {
            return;
        }
        try {
            Type setType = new TypeToken<HashSet<String>>() {}.getType();
            Set<String> loaded = gson.fromJson(Files.readString(dedupFile, StandardCharsets.UTF_8), setType);
            if (loaded != null) {
                seenDeliveries.addAll(loaded);
            }
        } catch (IOException ignored) {
        }
    }

    private void persistDedupSet() {
        try {
            Files.writeString(dedupFile, gson.toJson(seenDeliveries), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void emitAck(PublicationOuterClass.DeliveryEnvelope envelope) {
        PublicationOuterClass.DeliveryAck ack = PublicationOuterClass.DeliveryAck.newBuilder()
                .setDeliveryId(envelope.getDeliveryId())
                .setOwnerBrokerId(envelope.getOwnerBrokerId())
                .setNotifierId(task)
                .setAckTimestamp(Instant.now().toEpochMilli())
                .build();
        collector.emit("ack_stream", new Values((Object) ack.toByteArray()));
    }

    public void execute(Tuple input) {
        try
        {
            PublicationOuterClass.DeliveryEnvelope envelope = PublicationOuterClass.DeliveryEnvelope.parseFrom(input.getBinaryByField("notification_data"));
            if (!envelope.getTargetNotifierId().equals(task)) {
                return;
            }

            if (seenDeliveries.contains(envelope.getDeliveryId())) {
                emitAck(envelope);
                return;
            }

            PublicationOuterClass.Publication pub = PublicationOuterClass.Publication.parseFrom(envelope.getPublicationData());
            seenDeliveries.add(envelope.getDeliveryId());
            persistDedupSet();
            System.out.println(this.task + " Got publication: " + pub.toString());
            emitAck(envelope);
        } catch (InvalidProtocolBufferException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("ack_stream", new Fields("ack_data"));
    }
}