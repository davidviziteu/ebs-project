import java.util.Map;
import java.util.HashMap;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class NotifierBolt extends BaseRichBolt {
    private static final long serialVersionUID = 3;
    private String task;
    private static int SUBSCRIBER_COUNTER = 1;

    public NotifierBolt() {
        int subscriber = SUBSCRIBER_COUNTER++;
    }

    // remove template type qualifiers from conf declaration for Storm v1
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

        this.task = context.getThisComponentId();
        System.out.println("----- Started task: "+this.task);

    }

    public void execute(Tuple input) {
        try
        {
            PublicationOuterClass.Publication pub = PublicationOuterClass.Publication.parseFrom(input.getBinaryByField("notification_data"));
            System.out.println(this.task + " Got publication: " + pub.toString());
        } catch (InvalidProtocolBufferException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}