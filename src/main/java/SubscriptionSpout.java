import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.time.Instant;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.objenesis.ObjenesisHelper;

public class SubscriptionSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1;
    private static final int BROKER_COUNT = 10;
    private SpoutOutputCollector collector;
    private String task;
    private int i = 0;

    private String jsonPath;
    private transient HashRing hashRing;

    private ArrayList<ArrayList<FieldSubscription>> subscriptionList = new ArrayList<>();
    private long subSequence = 0L;

    SubscriptionSpout(String path)
    {
        super();
        jsonPath = path;
        // NOTE: do not instantiate HashRing here — Storm serializes the spout
    }

    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;
        this.task = context.getThisComponentId();

        // Initialize hash ring with available brokers (created here, after deserialization)
        this.hashRing = new HashRing();
        for (int j = 1; j <= BROKER_COUNT; j++) {
            hashRing.addNode("broker" + j);
        }

        Gson gson = new Gson();
        try
        {
            JsonReader reader = new JsonReader(new FileReader(jsonPath));
            InputData data = gson.fromJson(reader, InputData.class);
            for (int i=0;i<data.subscriptions.size();i++)
            {
                HashMap<String, SubscriptionData> sub = data.subscriptions.get(i);
                ArrayList<FieldSubscription> subscription = new ArrayList<>();
                for (String key : sub.keySet())
                {
                    subscription.add(new FieldSubscription(key, sub.get(key).operator, sub.get(key).value));
                }
                subscriptionList.add(subscription);
            }

        } catch (FileNotFoundException e)
        {
            System.out.println(e);
        }

        System.out.println("----- Started subscription spout task: "+this.task);
        System.out.println("----- Hash Ring initialized with " + hashRing.getNodeCount() + " brokers");

    }

    public void nextTuple() {
        if (i == this.subscriptionList.size())
            return;

        SubscriptionOuterClass.Subscription.Builder builder = SubscriptionOuterClass.Subscription.newBuilder();

        // Use hash ring to determine target broker
        String subscriptionKey = this.task + "-" + (subSequence++);
        String targetBroker = hashRing.getNode(subscriptionKey);
        builder.setSubId(subscriptionKey);
        builder.setSubscriberId(this.task);
        builder.setOwnerBrokerId(targetBroker);
        builder.setCreatedAt(Instant.now().toEpochMilli());
        for (FieldSubscription field_sub : subscriptionList.get(i))
        {
            SubscriptionOuterClass.Subscription.FieldSubscription.Builder field_builder = SubscriptionOuterClass.Subscription.FieldSubscription.newBuilder();
            field_builder.setKey(field_sub.field);
            field_builder.setOperator(field_sub.operator);
            field_builder.setValue(field_sub.value.toString());
            builder.addFieldSubscriptions(field_builder.build());
        }
        SubscriptionOuterClass.Subscription sub = builder.build();

        this.collector.emit(targetBroker, new Values((Object) sub.toByteArray()));
        i++;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (int i = 1; i <= BROKER_COUNT; i++) {
            declarer.declareStream("broker" + i, new Fields("subscription"));
        }
    }
}