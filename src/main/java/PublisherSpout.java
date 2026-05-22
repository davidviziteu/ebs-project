import java.io.FileNotFoundException;
import java.io.FileReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class PublisherSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1;
    private SpoutOutputCollector collector;
    private final List<Object> valueList = new ArrayList<>();
    private int i = 0;

    private String jsonPath;

    PublisherSpout(String path)
    {
        super();
        jsonPath = path;
    }

    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        Gson gson = new Gson();
        try
        {
            JsonReader reader = new JsonReader(new FileReader(jsonPath));
            InputData data = gson.fromJson(reader, InputData.class);
            for (PublicationData pub : data.publications)
            {
                PublicationOuterClass.Publication.Builder builder = PublicationOuterClass.Publication.newBuilder();
                builder.setStationid(pub.stationid);
                builder.setCity(pub.city);
                builder.setTemp(pub.temp);
                builder.setWind(pub.wind);
                PublicationOuterClass.Publication publication = builder.build();
                this.valueList.add(publication.toByteArray());
            }

        } catch (FileNotFoundException e)
        {
            System.out.println(e);
        }

        String task = context.getThisComponentId();
        System.out.println("----- Started publisher spout task: " + task);
    }

    public void nextTuple() {
        if(this.i == this.valueList.size())
        {
            i = 0;
//            return;
        }
        long timestamp = Instant.now().toEpochMilli();
        this.collector.emit(new Values(timestamp, this.valueList.get(i++)));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "publication_data"));
    }

}