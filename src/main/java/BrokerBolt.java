import com.google.gson.Gson;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BrokerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 2L;
    // Temporary toggles for incremental perf validation.
    // Re-enable only network signaling first (heartbeat/replication), keep durable failover state off.
    private static final boolean ENABLE_FAILOVER_STATE = true;
    private static final boolean ENABLE_HEARTBEAT = true;
    private static final boolean ENABLE_REPLICATION = false;
    private static final long HEARTBEAT_INTERVAL_MS = 1200L;
    private static final long HEARTBEAT_TIMEOUT_MS = 5000L;
    private static final long SNAPSHOT_INTERVAL_MS = 1000L;
    private static final int BROKER_COUNT = 10;

    private OutputCollector collector;
    private String brokerId;
    private String backupBrokerId;

    private final List<Float> temps = new ArrayList<>();
    private static final int WINDOW_SIZE = 2;

    private final Map<String, SubscriptionOuterClass.Subscription> localSubscriptions = new HashMap<>();
    private final Map<String, Map<String, SubscriptionOuterClass.Subscription>> replicatedSubscriptions = new HashMap<>();
    private final Map<String, PendingDelivery> pendingDeliveries = new HashMap<>();
    private final Set<String> currentRunPendingDeliveries = new HashSet<>();
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
    private boolean waitingForAcks = false;
    private final Deque<QueuedPublication> queuedPublications = new ArrayDeque<>();
    private boolean waitingForReplicationAck = false;
    private final Deque<PublicationOuterClass.BrokerReplication> pendingReplicationMessages = new ArrayDeque<>();
    private transient Path stateDir;
    private transient Path stateFile;
    private transient Gson gson;
    private long lastSnapshotMs = 0L;
    private boolean snapshotDirty = false;

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

    static class QueuedPublication {
        long timestamp;
        byte[] publicationData;

        QueuedPublication(long timestamp, byte[] publicationData) {
            this.timestamp = timestamp;
            this.publicationData = publicationData;
        }
    }

    static class BrokerSnapshot {
        long localSequence;
        Set<String> deliveredIds = new HashSet<>();
        Map<String, String> localSubscriptions = new HashMap<>();
        Map<String, PendingDelivery> pendingDeliveries = new HashMap<>();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.brokerId = context.getThisComponentId();
        this.simulateCrashBroker = System.getProperty("ebs.simulateCrashBroker", "");
        this.simulateCrashAfter = Long.parseLong(System.getProperty("ebs.simulateCrashAfter", "-1"));
        this.gson = new Gson();
        this.stateDir = Paths.get(System.getProperty("java.io.tmpdir"), "ebs-state");
        this.stateFile = stateDir.resolve(this.brokerId + ".json");
        try {
            Files.createDirectories(stateDir);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create broker state dir", e);
        }

        for (int i = 1; i <= BROKER_COUNT; i++) {
            String primary = "broker" + i;
            String backup = "broker" + ((i % BROKER_COUNT) + 1);
            this.backupForPrimary.put(primary, backup);
        }
        for (Map.Entry<String, String> e : backupForPrimary.entrySet()) {
            primaryForBackup.put(e.getValue(), e.getKey());
        }
        this.backupBrokerId = backupForPrimary.get(this.brokerId);
        String watchedPrimary = primaryForBackup.get(this.brokerId);
        if (watchedPrimary != null) {
            lastHeartbeat.put(watchedPrimary, Instant.now().toEpochMilli());
        }

        System.out.println("----- Started broker task: " + this.brokerId + " with backup " + this.backupBrokerId);
    }

    private void persistState() {
        snapshotDirty = true;
        flushSnapshotIfDue(false);
    }

    private void flushSnapshotIfDue(boolean force) {
        if (!snapshotDirty) {
            return;
        }
        long now = Instant.now().toEpochMilli();
        if (!force && now - lastSnapshotMs < SNAPSHOT_INTERVAL_MS) {
            return;
        }
        BrokerSnapshot snapshot = new BrokerSnapshot();
        snapshot.localSequence = this.localSequence;
        snapshot.deliveredIds.addAll(this.deliveredIds);
        snapshot.pendingDeliveries.putAll(this.pendingDeliveries);
        for (Map.Entry<String, SubscriptionOuterClass.Subscription> e : localSubscriptions.entrySet()) {
            snapshot.localSubscriptions.put(e.getKey(), Base64.getEncoder().encodeToString(e.getValue().toByteArray()));
        }
        try {
            Files.writeString(stateFile, gson.toJson(snapshot), StandardCharsets.UTF_8);
            snapshotDirty = false;
            lastSnapshotMs = now;
        } catch (IOException ex) {
            throw new RuntimeException("Failed to write broker snapshot", ex);
        }
    }

    private BrokerSnapshot loadSnapshotForBroker(String broker) {
        Path file = stateDir.resolve(broker + ".json");
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return gson.fromJson(Files.readString(file, StandardCharsets.UTF_8), BrokerSnapshot.class);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read broker snapshot for " + broker, ex);
        }
    }

    private void adoptPrimaryFromSnapshot(String primaryBroker) {
        BrokerSnapshot snapshot = loadSnapshotForBroker(primaryBroker);
        if (snapshot == null) {
            return;
        }
        this.deliveredIds.addAll(snapshot.deliveredIds);
        Map<String, SubscriptionOuterClass.Subscription> primarySubs = new HashMap<>();
        for (Map.Entry<String, String> e : snapshot.localSubscriptions.entrySet()) {
            try {
                primarySubs.put(
                        e.getKey(),
                        SubscriptionOuterClass.Subscription.parseFrom(Base64.getDecoder().decode(e.getValue()))
                );
            } catch (InvalidProtocolBufferException ex) {
                throw new RuntimeException("Invalid subscription in primary snapshot", ex);
            }
        }
        replicatedSubscriptions.put(primaryBroker, primarySubs);
        Map<String, PendingDelivery> primaryPending = new HashMap<>(snapshot.pendingDeliveries);
        replicatedPending.put(primaryBroker, primaryPending);
        replayReplicatedPending(primaryBroker);
    }

    private void maybeEmitHeartbeat() {
        if (!ENABLE_HEARTBEAT) {
            return;
        }
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
        if (!ENABLE_FAILOVER_STATE) {
            return;
        }
        long now = Instant.now().toEpochMilli();
        String primary = primaryForBackup.get(brokerId);
        if (primary == null) {
            return;
        }
        long last = lastHeartbeat.getOrDefault(primary, 0L);
        if (last > 0 && now - last > HEARTBEAT_TIMEOUT_MS && !failedPrimaries.contains(primary)) {
            failedPrimaries.add(primary);
            System.out.println(brokerId + " activated failover for " + primary);
            adoptPrimaryFromSnapshot(primary);
        }
    }

    private void emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType type, String key, byte[] payload) {
        if (!ENABLE_REPLICATION) {
            return;
        }
        PublicationOuterClass.BrokerReplication msg = PublicationOuterClass.BrokerReplication.newBuilder()
                .setType(type)
                .setSourceBrokerId(brokerId)
                .setTargetBrokerId(backupBrokerId)
                .setTimestamp(Instant.now().toEpochMilli())
                .setKey(key == null ? "" : key)
                .setPayload(ByteString.copyFrom(payload))
                .build();
        if (waitingForReplicationAck) {
            pendingReplicationMessages.addLast(msg);
            return;
        }
        collector.emit("replication_stream", new Values(backupBrokerId, msg.toByteArray()));
        waitingForReplicationAck = true;
    }

    private void flushPendingReplications() {
        if (!ENABLE_REPLICATION || waitingForReplicationAck || pendingReplicationMessages.isEmpty()) {
            return;
        }
        PublicationOuterClass.BrokerReplication next = pendingReplicationMessages.pollFirst();
        collector.emit("replication_stream", new Values(next.getTargetBrokerId(), next.toByteArray()));
        waitingForReplicationAck = true;
    }

    private void emitReplicationAck(String targetBrokerId) {
        if (!ENABLE_REPLICATION) {
            return;
        }
        PublicationOuterClass.BrokerReplication ack = PublicationOuterClass.BrokerReplication.newBuilder()
                .setType(PublicationOuterClass.BrokerReplication.ReplicationType.CHECKPOINT)
                .setSourceBrokerId(brokerId)
                .setTargetBrokerId(targetBrokerId)
                .setTimestamp(Instant.now().toEpochMilli())
                .setKey("replication-ack")
                .setPayload(ByteString.EMPTY)
                .build();
        collector.emit("replication_stream", new Values(targetBrokerId, ack.toByteArray()));
    }

    private void handleSubscription(byte[] rawSub) throws InvalidProtocolBufferException {
        SubscriptionOuterClass.Subscription subscription = SubscriptionOuterClass.Subscription.parseFrom(rawSub);
        localSubscriptions.put(subscription.getSubId(), subscription);
        persistState();
        emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType.SUBSCRIPTION, subscription.getSubId(), rawSub);
    }

    private void handleAck(byte[] rawAck) throws InvalidProtocolBufferException {
        if (!ENABLE_FAILOVER_STATE) {
            return;
        }
        PublicationOuterClass.DeliveryAck ack = PublicationOuterClass.DeliveryAck.parseFrom(rawAck);
        if (!ack.getOwnerBrokerId().equals(brokerId)) {
            return;
        }
        pendingDeliveries.remove(ack.getDeliveryId());
        currentRunPendingDeliveries.remove(ack.getDeliveryId());
        deliveredIds.add(ack.getDeliveryId());
        persistState();
        emitReplication(PublicationOuterClass.BrokerReplication.ReplicationType.DELIVERY_ACK, ack.getDeliveryId(), rawAck);
        if (waitingForAcks && currentRunPendingDeliveries.isEmpty()) {
            waitingForAcks = false;
            drainQueuedPublications();
        }
    }

    private void handleReplication(byte[] rawReplication) throws InvalidProtocolBufferException {
        if (!ENABLE_REPLICATION) {
            return;
        }
        PublicationOuterClass.BrokerReplication msg = PublicationOuterClass.BrokerReplication.parseFrom(rawReplication);
        if (!msg.getTargetBrokerId().equals(brokerId)) {
            return;
        }
        if (msg.getType() == PublicationOuterClass.BrokerReplication.ReplicationType.CHECKPOINT) {
            waitingForReplicationAck = false;
            flushPendingReplications();
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
            default:
                break;
        }
        emitReplicationAck(source);
        persistState();
    }

    private void handleHeartbeat(byte[] rawHeartbeat) throws InvalidProtocolBufferException {
        if (!ENABLE_HEARTBEAT) {
            return;
        }
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
        // Collapse multiple matching subscriptions into a single delivery per pubId+notifier.
        String notifierId = subscription.getSubscriberId().replace("subscription", "notifier");
        String deliveryId = publication.getPubId() + "::" + notifierId;
        if (ENABLE_FAILOVER_STATE && (deliveredIds.contains(deliveryId) || pendingDeliveries.containsKey(deliveryId))) {
            return;
        }

        PublicationOuterClass.DeliveryEnvelope envelope = PublicationOuterClass.DeliveryEnvelope.newBuilder()
                .setDeliveryId(deliveryId)
                .setPubId(publication.getPubId())
                .setSubId(subscription.getSubId())
                .setOwnerBrokerId(brokerId)
                .setTargetNotifierId(notifierId)
                .setSeqNo(++localSequence)
                .setPublicationData(ByteString.copyFrom(publication.toByteArray()))
                .build();

        if (ENABLE_FAILOVER_STATE) {
            PendingDelivery pending = toPending(envelope);
            pendingDeliveries.put(deliveryId, pending);
            currentRunPendingDeliveries.add(deliveryId);
            persistState();
        }
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
        int pendingBefore = currentRunPendingDeliveries.size();
        publicationCounter++;
        if (ENABLE_FAILOVER_STATE
                && !simulatedDown
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
        if (ENABLE_FAILOVER_STATE) {
            for (String failedPrimary : failedPrimaries) {
                Map<String, SubscriptionOuterClass.Subscription> subs = replicatedSubscriptions.getOrDefault(failedPrimary, new HashMap<>());
                for (SubscriptionOuterClass.Subscription sub : subs.values()) {
                    if (matches(publication, sub)) {
                        emitDelivery(publication, sub);
                    }
                }
            }
        }
        maybeEmitHeartbeat();
        if (ENABLE_FAILOVER_STATE && currentRunPendingDeliveries.size() > pendingBefore) {
            waitingForAcks = true;
        }
    }

    private void handlePublisherTuple(long timestamp, byte[] publicationData, boolean endOfStream) throws InvalidProtocolBufferException {
        if (endOfStream) {
            return;
        }
        if (ENABLE_FAILOVER_STATE && waitingForAcks) {
            queuedPublications.addLast(new QueuedPublication(timestamp, publicationData));
            return;
        }
        PublicationOuterClass.Publication pub = PublicationOuterClass.Publication.parseFrom(publicationData);
        processPublication(pub, timestamp);
        if (!waitingForAcks) {
            drainQueuedPublications();
        }
    }

    private void drainQueuedPublications() {
        if (!ENABLE_FAILOVER_STATE) {
            queuedPublications.clear();
            return;
        }
        while (!waitingForAcks && !queuedPublications.isEmpty()) {
            QueuedPublication queued = queuedPublications.pollFirst();
            try {
                PublicationOuterClass.Publication pub = PublicationOuterClass.Publication.parseFrom(queued.publicationData);
                processPublication(pub, queued.timestamp);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
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
                boolean endOfStream = input.contains("end_of_stream") && input.getBooleanByField("end_of_stream");
                handlePublisherTuple(input.getLongByField("timestamp"), input.getBinaryByField("publication_data"), endOfStream);
            }
            checkFailover();
            flushSnapshotIfDue(false);
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