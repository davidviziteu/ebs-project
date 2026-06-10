import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NotifierBolt extends BaseRichBolt {
    private static final long serialVersionUID = 3;
    // Re-enabled: dedup + ack flow for failover-state testing.
    private static final boolean ENABLE_CRASH_TOLERANCE = true;
    private String task;
    private OutputCollector collector;
    private final Set<String> seenDeliveries = new HashSet<>();

    // remove template type qualifiers from conf declaration for Storm v1
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.task = context.getThisComponentId();
        loadDedupSet();
        System.out.println("----- Started task: "+this.task);

    }

    private void loadDedupSet() {
        // Intentionally no-op: dedup state is in-memory only.
    }

    private void persistDedupSet() {
        // Intentionally no-op: dedup state is in-memory only.
    }

    private void emitAck(PublicationOuterClass.DeliveryEnvelope envelope) {
        if (!ENABLE_CRASH_TOLERANCE) {
            return;
        }
        PublicationOuterClass.DeliveryAck ack = PublicationOuterClass.DeliveryAck.newBuilder()
                .setDeliveryId(envelope.getDeliveryId())
                .setOwnerBrokerId(envelope.getOwnerBrokerId())
                .setNotifierId(task)
                .setAckTimestamp(Instant.now().toEpochMilli())
                .build();
        collector.emit("ack_stream", new Values(ack.getOwnerBrokerId(), ack.toByteArray()));
    }

    public void execute(Tuple input) {
        try
        {
            PublicationOuterClass.DeliveryEnvelope envelope = PublicationOuterClass.DeliveryEnvelope.parseFrom(input.getBinaryByField("notification_data"));
            if (!envelope.getTargetNotifierId().equals(task)) {
                return;
            }

            if (ENABLE_CRASH_TOLERANCE && seenDeliveries.contains(envelope.getDeliveryId())) {
                emitAck(envelope);
                return;
            }

            PublicationOuterClass.Publication pub = PublicationOuterClass.Publication.parseFrom(envelope.getPublicationData());
            if (ENABLE_CRASH_TOLERANCE) {
                seenDeliveries.add(envelope.getDeliveryId());
                persistDedupSet();
            }
            System.out.println(this.task + " Got publication: " + pub.toString());
            emitAck(envelope);
        } catch (InvalidProtocolBufferException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("ack_stream", new Fields("owner_broker_id", "ack_data"));
    }
}