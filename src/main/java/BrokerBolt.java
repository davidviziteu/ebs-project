import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BrokerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 2L;
    private static final long HEARTBEAT_INTERVAL_MS = 1200L;
    private static final long HEARTBEAT_TIMEOUT_MS = 5000L;

    private OutputCollector collector;
    private String brokerId;
    private String backupBrokerId;

    private final List<Float> temps = new ArrayList<>();
    private static final int WINDOW_SIZE = 2;

    private final Map<String, SubscriptionOuterClass.Subscription> localSubscriptions = new HashMap<>();
    private final Map<String, Map<String, SubscriptionOuterClass.Subscription>> replicatedSubscriptions = new HashMap<>();
    private final Map<String, PendingDelivery> pendingDeliveries = new HashMap<>();
    private final Map<String, Map<String, PendingDelivery>> replicatedPending = new HashMap<>();
    private final Set<String> deliveredIds = new HashSet<>();
    private final Map<String, Long> lastHeartbeat = new HashMap<>();
    private final Set<String> failedPrimaries = new HashSet<>();
    private final Map<String, String> backupForPrimary = new HashMap<>();
    private final Map<String, String> primaryForBackup = new HashMap<>();

    private long lastHeartbeatSentMs = 0;
    private long localSequence = 0;
    private long publicationCounter = 0;
    private boolean simulatedDown = false;
    private String simulateCrashBroker;
    private long simulateCrashAfter = -1;
    private transient Path stateFile;
    private transient Gson gson;

    static class PendingDelivery {
        String deliveryId;
        String ownerBrokerId;
        String targetNotifierId;
        String pubId;
        String subId;
        long seqNo;
        String publicationBase64;
        long createdAt;
    }

    static class BrokerSnapshot {
        Map<String, String> localSubscriptions = new HashMap<>();
        Map<String, Map<String, String>> replicatedSubscriptions = new HashMap<>();
        Map<String, PendingDelivery> pendingDeliveries = new HashMap<>();
        Map<String, Map<String, PendingDelivery>> replicatedPending = new HashMap<>();
        Set<String> deliveredIds = new HashSet<>();
        long localSequence;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.brokerId = context.getThisComponentId();
        this.stateFile = Paths.get(System.getProperty("java.io.tmpdir"), "ebs-" + brokerId + "-state.json");
        this.gson = new Gson();
        this.simulateCrashBroker = System.getProperty("ebs.simulateCrashBroker", "");
        this.simulateCrashAfter = Long.parseLong(System.getProperty("ebs.simulateCrashAfter", "-1"));

        this.backupForPrimary.put("broker1", "broker2");
        this.backupForPrimary.put("broker2", "broker3");
        this.backupForPrimary.put("broker3", "broker1");
        for (Map.Entry<String, String> e : backupForPrimary.entrySet()) {
            primaryForBackup.put(e.getValue(), e.getKey());
        }
        this.backupBrokerId = backupForPrimary.get(this.brokerId);
        String watchedPrimary = primaryForBackup.get(this.brokerId);
        if (watchedPrimary != null) {
            lastHeartbeat.put(watchedPrimary, Instant.now().toEpochMilli());
        }

        loadState();
        System.out.println("----- Started broker task: " + this.brokerId + " with backup " + this.backupBrokerId);
    }

    private void loadState() {
        if (!Files.exists(stateFile)) {
            return;
        }
        try {
            String json = Files.readString(stateFile, StandardCharsets.UTF_8);
            BrokerSnapshot snapshot = gson.fromJson(json, BrokerSnapshot.class);
            if (snapshot == null) {
                return;
            }
            this.localSequence = snapshot.localSequence;
            this.deliveredIds.addAll(snapshot.deliveredIds);
            this.pendingDeliveries.putAll(snapshot.pendingDeliveries);
            this.replicatedPending.putAll(snapshot.replicatedPending);
            for (Map.Entry<String, String> e : snapshot.localSubscriptions.entrySet()) {
                localSubscriptions.put(e.getKey(), SubscriptionOuterClass.Subscription.parseFrom(Base64.getDecoder().decode(e.getValue())));
            }
            for (Map.Entry<String, Map<String, String>> owner : snapshot.replicatedSubscriptions.entrySet()) {
                Map<String, SubscriptionOuterClass.Subscription> ownerSubs = new HashMap<>();
                for (Map.Entry<String, String> sub : owner.getValue().entrySet()) {
                    ownerSubs.put(sub.getKey(), SubscriptionOuterClass.Subscription.parseFrom(Base64.getDecoder().decode(sub.getValue())));
                }
                replicatedSubscriptions.put(owner.getKey(), ownerSubs);
            }
        } catch (Exception ex) {
            System.out.println("Failed to load broker state for " + brokerId + ": " + ex.getMessage());
        }
    }

    private void persistState() {
        try {
            BrokerSnapshot snapshot = new BrokerSnapshot();
            snapshot.localSequence = this.localSequence;
            snapshot.deliveredIds = new HashSet<>(this.deliveredIds);
            snapshot.pendingDeliveries = new HashMap<>(this.pendingDeliveries);
            snapshot.replicatedPending = new HashMap<>(this.replicatedPending);
            for (Map.Entry<String, SubscriptionOuterClass.Subscription> e : localSubscriptions.entrySet()) {
                snapshot.localSubscriptions.put(e.getKey(), Base64.getEncoder().encodeToString(e.getValue().toByteArray()));
            }
            for (Map.Entry<String, Map<String, SubscriptionOuterClass.Subscription>> owner : replicatedSubscriptions.entrySet()) {
                Map<String, String> serialized = new HashMap<>();
                for (Map.Entry<String, SubscriptionOuterClass.Subscription> sub : owner.getValue().entrySet()) {
                    serialized.put(sub.getKey(), Base64.getEncoder().encodeToString(sub.getValue().toByteArray()));
                }
                snapshot.replicatedSubscriptions.put(owner.getKey(), serialized);
            }
            Files.writeString(stateFile, gson.toJson(snapshot), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to persist broker state for " + brokerId, ex);
        }
    }

    private void maybeEmitHeartbeat() {
        long now = Instant.now().toEpochMilli();
        if (now - lastHeartbeatSentMs < HEARTBEAT_INTERVAL_MS) {
            return;
        }
        lastHeartbeatSentMs = now;
        PublicationOuterClass.Heartbeat heartbeat = PublicationOuterClass.Heartbeat.newBuilder()
                .setSourceBrokerId(brokerId)
                .setTargetBrokerId(backupBrokerId)
                .setTimestamp(now)
                .setSeqNo(++localSequence)
                .build();
        collector.emit("heartbeat_stream", new Values(backupBrokerId, heartbeat.toByteArray()));
    }

    private void checkFailover() {
        long now = Instant.now().toEpochMilli();
        String primary = primaryForBackup.get(brokerId);
        if (primary == null) {
            return;
        }
        long last = lastHeartbeat.getOrDefault(primary, 0L);
        if (last > 0 && now - last > HEARTBEAT_TIMEOUT_MS && !failedPrimaries.contains(primary)) {
            failedPrimaries.add(primary);
            System.out.println(brokerId + " activated failover for " + primary);
            replayReplicatedPending(primary);
        }
    }

    private void emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType type, String key, byte[] payload) {
        PublicationOuterClass.BrokerReplication msg = PublicationOuterClass.BrokerReplication.newBuilder()
                .setType(type)
                .setSourceBrokerId(brokerId)
                .setTargetBrokerId(backupBrokerId)
                .setTimestamp(Instant.now().toEpochMilli())
                .setKey(key == null ? "" : key)
                .setPayload(ByteString.copyFrom(payload))
                .build();
        collector.emit("replication_stream", new Values(backupBrokerId, msg.toByteArray()));
    }

    private void handleSubscription(byte[] rawSub) throws InvalidProtocolBufferException {
        SubscriptionOuterClass.Subscription subscription = SubscriptionOuterClass.Subscription.parseFrom(rawSub);
        localSubscriptions.put(subscription.getSubId(), subscription);
        persistState();
        emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType.SUBSCRIPTION, subscription.getSubId(), rawSub);
    }

    private void handleAck(byte[] rawAck) throws InvalidProtocolBufferException {
        PublicationOuterClass.DeliveryAck ack = PublicationOuterClass.DeliveryAck.parseFrom(rawAck);
        if (!ack.getOwnerBrokerId().equals(brokerId)) {
            return;
        }
        pendingDeliveries.remove(ack.getDeliveryId());
        deliveredIds.add(ack.getDeliveryId());
        persistState();
        emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType.DELIVERY_ACK, ack.getDeliveryId(), rawAck);
    }

    private void handleReplication(byte[] rawReplication) throws InvalidProtocolBufferException {
        PublicationOuterClass.BrokerReplication msg = PublicationOuterClass.BrokerReplication.parseFrom(rawReplication);
        if (!msg.getTargetBrokerId().equals(brokerId)) {
            return;
        }
        String source = msg.getSourceBrokerId();
        switch (msg.getType()) {
            case HEARTBEAT:
                lastHeartbeat.put(source, msg.getTimestamp());
                break;
            case SUBSCRIPTION:
                SubscriptionOuterClass.Subscription sub = SubscriptionOuterClass.Subscription.parseFrom(msg.getPayload());
                replicatedSubscriptions.computeIfAbsent(source, k -> new HashMap<>()).put(msg.getKey(), sub);
                break;
            case PENDING_DELIVERY:
                PublicationOuterClass.DeliveryEnvelope env = PublicationOuterClass.DeliveryEnvelope.parseFrom(msg.getPayload());
                replicatedPending.computeIfAbsent(source, k -> new HashMap<>()).put(env.getDeliveryId(), toPending(env));
                break;
            case DELIVERY_ACK:
                replicatedPending.computeIfAbsent(source, k -> new HashMap<>()).remove(msg.getKey());
                deliveredIds.add(msg.getKey());
                break;
            case CHECKPOINT:
            default:
                break;
        }
        persistState();
    }

    private void handleHeartbeat(byte[] rawHeartbeat) throws InvalidProtocolBufferException {
        PublicationOuterClass.Heartbeat heartbeat = PublicationOuterClass.Heartbeat.parseFrom(rawHeartbeat);
        if (!heartbeat.getTargetBrokerId().equals(brokerId)) {
            return;
        }
        lastHeartbeat.put(heartbeat.getSourceBrokerId(), heartbeat.getTimestamp());
    }

    private PendingDelivery toPending(PublicationOuterClass.DeliveryEnvelope envelope) {
        PendingDelivery p = new PendingDelivery();
        p.deliveryId = envelope.getDeliveryId();
        p.ownerBrokerId = envelope.getOwnerBrokerId();
        p.targetNotifierId = envelope.getTargetNotifierId();
        p.pubId = envelope.getPubId();
        p.subId = envelope.getSubId();
        p.seqNo = envelope.getSeqNo();
        p.publicationBase64 = Base64.getEncoder().encodeToString(envelope.getPublicationData().toByteArray());
        p.createdAt = Instant.now().toEpochMilli();
        return p;
    }

    private PublicationOuterClass.DeliveryEnvelope fromPending(PendingDelivery pending) {
        return PublicationOuterClass.DeliveryEnvelope.newBuilder()
                .setDeliveryId(pending.deliveryId)
                .setOwnerBrokerId(brokerId)
                .setTargetNotifierId(pending.targetNotifierId)
                .setPubId(pending.pubId)
                .setSubId(pending.subId)
                .setSeqNo(pending.seqNo)
                .setPublicationData(ByteString.copyFrom(Base64.getDecoder().decode(pending.publicationBase64)))
                .build();
    }

    private String getPublicationField(PublicationOuterClass.Publication publication, String field) {
        switch (field) {
            case "stationid":
                return publication.getStationid();
            case "temp":
                return publication.getTemp();
            case "city":
                return publication.getCity();
            case "wind":
                return publication.getWind();
            default:
                return "";
        }
    }

    private boolean matches(PublicationOuterClass.Publication publication, SubscriptionOuterClass.Subscription subscription) {
        for (SubscriptionOuterClass.Subscription.FieldSubscription filter : subscription.getFieldSubscriptionsList()) {
            String value = getPublicationField(publication, filter.getKey());
            String expected = filter.getValue();
            switch (filter.getOperator()) {
                case "=":
                    if (!value.equals(expected)) {
                        return false;
                    }
                    break;
                case "!=":
                    if (value.equals(expected)) {
                        return false;
                    }
                    break;
                case "<":
                    if (!(Float.parseFloat(value) < Float.parseFloat(expected))) {
                        return false;
                    }
                    break;
                case ">":
                    if (!(Float.parseFloat(value) > Float.parseFloat(expected))) {
                        return false;
                    }
                    break;
                case "<=":
                    if (!(Float.parseFloat(value) <= Float.parseFloat(expected))) {
                        return false;
                    }
                    break;
                case ">=":
                    if (!(Float.parseFloat(value) >= Float.parseFloat(expected))) {
                        return false;
                    }
                    break;
                case "~":
                    if (temps.size() < WINDOW_SIZE) {
                        return false;
                    }
                    float avg = 0;
                    for (float temp : temps) {
                        avg += temp;
                    }
                    avg /= temps.size();
                    if (!(avg == Float.parseFloat(expected))) {
                        return false;
                    }
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

    private void emitDelivery(PublicationOuterClass.Publication publication, SubscriptionOuterClass.Subscription subscription) {
        String deliveryId = publication.getPubId() + "::" + subscription.getSubId();
        if (deliveredIds.contains(deliveryId) || pendingDeliveries.containsKey(deliveryId)) {
            return;
        }

        String notifierId = subscription.getSubscriberId().replace("subscription", "notifier");
        PublicationOuterClass.DeliveryEnvelope envelope = PublicationOuterClass.DeliveryEnvelope.newBuilder()
                .setDeliveryId(deliveryId)
                .setPubId(publication.getPubId())
                .setSubId(subscription.getSubId())
                .setOwnerBrokerId(brokerId)
                .setTargetNotifierId(notifierId)
                .setSeqNo(++localSequence)
                .setPublicationData(ByteString.copyFrom(publication.toByteArray()))
                .build();

        PendingDelivery pending = toPending(envelope);
        pendingDeliveries.put(deliveryId, pending);
        persistState();
        emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType.PENDING_DELIVERY, deliveryId, envelope.toByteArray());

        Stats.match_number++;
        collector.emit(notifierId, new Values((Object) envelope.toByteArray()));
    }

    private void replayReplicatedPending(String failedPrimary) {
        Map<String, PendingDelivery> failedPending = replicatedPending.getOrDefault(failedPrimary, new HashMap<>());
        List<PendingDelivery> ordered = new ArrayList<>(failedPending.values());
        ordered.sort(Comparator.comparingLong(p -> p.seqNo));
        for (PendingDelivery pending : ordered) {
            if (deliveredIds.contains(pending.deliveryId)) {
                continue;
            }
            PendingDelivery adopted = pending;
            adopted.ownerBrokerId = brokerId;
            pendingDeliveries.put(adopted.deliveryId, adopted);
            collector.emit(adopted.targetNotifierId, new Values((Object) fromPending(adopted).toByteArray()));
        }
        persistState();
    }

    private void processPublication(PublicationOuterClass.Publication publication, long sourceTimestamp) {
        publicationCounter++;
        if (!simulatedDown
                && brokerId.equals(simulateCrashBroker)
                && simulateCrashAfter >= 0
                && publicationCounter >= simulateCrashAfter) {
            simulatedDown = true;
            System.out.println(brokerId + " entered simulated crash mode after " + publicationCounter + " publications.");
        }
        if (simulatedDown) {
            return;
        }

        long now = Instant.now().toEpochMilli();
        Stats.latency += (now - sourceTimestamp);
        Stats.publications_number++;

        if (temps.size() == WINDOW_SIZE) {
            temps.clear();
        }
        temps.add(Float.parseFloat(publication.getTemp()));

        for (SubscriptionOuterClass.Subscription sub : localSubscriptions.values()) {
            if (matches(publication, sub)) {
                emitDelivery(publication, sub);
            }
        }
        for (String failedPrimary : failedPrimaries) {
            Map<String, SubscriptionOuterClass.Subscription> subs = replicatedSubscriptions.getOrDefault(failedPrimary, new HashMap<>());
            for (SubscriptionOuterClass.Subscription sub : subs.values()) {
                if (matches(publication, sub)) {
                    emitDelivery(publication, sub);
                }
            }
        }
        maybeEmitHeartbeat();
    }

    @Override
    public void execute(Tuple input) {
        try {
            String source = input.getSourceComponent();
            String stream = input.getSourceStreamId();

            if (source.startsWith("subscription")) {
                if (simulatedDown) {
                    return;
                }
                handleSubscription(input.getBinaryByField("subscription"));
            } else if (source.startsWith("notifier") && "ack_stream".equals(stream)) {
                if (simulatedDown) {
                    return;
                }
                handleAck(input.getBinaryByField("ack_data"));
            } else if (source.startsWith("broker") && "replication_stream".equals(stream)) {
                if (simulatedDown) {
                    return;
                }
                handleReplication(input.getBinaryByField("replication_data"));
            } else if (source.startsWith("broker") && "heartbeat_stream".equals(stream)) {
                if (simulatedDown) {
                    return;
                }
                handleHeartbeat(input.getBinaryByField("heartbeat_data"));
            } else if ("publisher_spout".equals(source)) {
                if (input.contains("end_of_stream") && input.getBooleanByField("end_of_stream")) {
                    return;
                }
                PublicationOuterClass.Publication pub = PublicationOuterClass.Publication.parseFrom(input.getBinaryByField("publication_data"));
                processPublication(pub, input.getLongByField("timestamp"));
            }
            checkFailover();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notifier1", new Fields("notification_data"));
        declarer.declareStream("notifier2", new Fields("notification_data"));
        declarer.declareStream("notifier3", new Fields("notification_data"));
        declarer.declareStream("replication_stream", new Fields("target_broker", "replication_data"));
        declarer.declareStream("heartbeat_stream", new Fields("target_broker", "heartbeat_data"));
    }
}