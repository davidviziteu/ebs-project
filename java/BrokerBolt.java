import java.time.Instant;
import java.util.*;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.zookeeper.data.Stat;

public class BrokerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 2;

    private OutputCollector collector;
    private String task;
    private List<Float> temps = new ArrayList<>();
    static final private int WINDOW_SIZE = 2;

    private HashMap<String, List<List<FieldSubscription>>> subscriptions = new HashMap<>();

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.task = context.getThisComponentId();
        System.out.println("----- Started task: " + this.task);
    }

    String get_publication_field(PublicationOuterClass.Publication publication, String field)
    {
        switch (field)
        {
            case "stationid":
                return publication.getStationid();
            case "temp":
                return publication.getTemp();
            case "city":
                return publication.getCity();
            case "wind":
                return publication.getWind();
            default:
                System.out.println("Invalid field");
                System.exit(1);
        }
        return null;
    }

    List<FieldSubscription> convert_field_subscriptions(List<SubscriptionOuterClass.Subscription.FieldSubscription> list)
    {
        List<FieldSubscription> result = new ArrayList<>();
        for (SubscriptionOuterClass.Subscription.FieldSubscription sub : list)
        {
            result.add(new FieldSubscription(sub.getKey(), sub.getOperator(), sub.getValue()));
        }
        return result;
    }

    public void execute(Tuple input) {
        String sourceName = input.getSourceComponent();
        if (sourceName.contains("subscription")) {
            System.out.println(this.task + " got subscription " + sourceName);
            String sourceId = sourceName.substring(sourceName.length() - 1);
            SubscriptionOuterClass.Subscription subscription_data = null;
            try
            {
                subscription_data = SubscriptionOuterClass.Subscription.parseFrom(input.getBinaryByField("subscription"));
                List<FieldSubscription> subscription = convert_field_subscriptions(subscription_data.getFieldSubscriptionsList());
                if (!subscriptions.containsKey(sourceId)) {
                    List<List<FieldSubscription>> newList = new ArrayList<>();
                    subscriptions.put(sourceId, newList);
                }
                List<List<FieldSubscription>> existingList = subscriptions.get(sourceId);
                existingList.add(subscription);
                subscriptions.put(sourceId, existingList);
            } catch (InvalidProtocolBufferException e)
            {
                System.out.println(e);
                throw new RuntimeException(e);
            }

        } else {
            System.out.println(this.task + " got publication " + sourceName);
            long timestamp = Instant.now().toEpochMilli();
            long latency = timestamp - input.getLong(0);
            Stats.latency += latency;
            Stats.publications_number++;
            if (this.temps.size() == WINDOW_SIZE)
                this.temps.clear();
            try
            {
                PublicationOuterClass.Publication publication_data = PublicationOuterClass.Publication.parseFrom(input.getBinaryByField("publication_data"));
                this.temps.add(Float.parseFloat(publication_data.getTemp()));
                for (String boltId : subscriptions.keySet()) {
                    List<List<FieldSubscription>> aList = subscriptions.get(boltId);
                    for (List<FieldSubscription> subscriptions1 : aList) {
                        boolean ok = true;
                        for (FieldSubscription fieldSubscription : subscriptions1) {
                            if (fieldSubscription.operator.equals("=")) {
                                String publication = get_publication_field(publication_data, fieldSubscription.field);
                                if (!publication.equals(fieldSubscription.value)) {
                                    ok = false;
                                    break;
                                }
                            } else if (fieldSubscription.operator.equals("!=")) {
                                String publication = get_publication_field(publication_data, fieldSubscription.field);
                                if (publication.equals(fieldSubscription.value)) {
                                    ok = false;
                                    break;
                                }
                            } else if (fieldSubscription.operator.equals("<")) {
                                float publication = Float.parseFloat(get_publication_field(publication_data, fieldSubscription.field));
                                float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                                if (!(publication < subscriptionValue)) {
                                    ok = false;
                                    break;
                                }
                            } else if (fieldSubscription.operator.equals(">")) {
                                float publication = Float.parseFloat(get_publication_field(publication_data, fieldSubscription.field));
                                float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                                if (!(publication > subscriptionValue)) {
                                    ok = false;
                                    break;
                                }
                            } else if (fieldSubscription.operator.equals("<=")) {
                                float publication = Float.parseFloat(get_publication_field(publication_data, fieldSubscription.field));
                                float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                                if (!(publication <= subscriptionValue)) {
                                    ok = false;
                                    break;
                                }
                            } else if (fieldSubscription.operator.equals(">=")) {
                                float publication = Float.parseFloat(get_publication_field(publication_data, fieldSubscription.field));
                                float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                                if (!(publication >= subscriptionValue)) {
                                    ok = false;
                                    break;
                                }
                            } else if (fieldSubscription.operator.equals("~")) {
                                if (this.temps.size() < WINDOW_SIZE) {
                                    ok = false;
                                    break;
                                } else {
                                    float publication = 0;
                                    for (float temp : this.temps) {
                                        publication += temp;
                                    }
                                    publication /= temps.size();
                                    float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                                    if (!(publication == subscriptionValue)) {
                                        ok = false;
                                        break;
                                    }
                                }
                            } else {
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            Stats.match_number++;
                            collector.emit("notifier" + boltId, new Values((Object) publication_data.toByteArray()));
                        }
                    }
                }
            } catch (InvalidProtocolBufferException e)
            {
                System.out.println(e);
            }

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notifier1", new Fields("notification_data"));
        declarer.declareStream("notifier2", new Fields("notification_data"));
        declarer.declareStream("notifier3", new Fields("notification_data"));
    }

}