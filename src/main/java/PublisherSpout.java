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
    private static final int BROKER_COUNT = 10;
    private static final long EMIT_DELAY_MS = 1L;
    private SpoutOutputCollector collector;
    private final List<PublicationOuterClass.Publication> valueList = new ArrayList<>();
    private int i = 0;
    private long publicationSeq = 0L;
    private boolean endOfStreamEmitted = false;

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
                this.valueList.add(publication);
            }

        } catch (FileNotFoundException e)
        {
            System.out.println(e);
        }

        String task = context.getThisComponentId();
        System.out.println("----- Started publisher spout task: " + task);
    }

    public void nextTuple() {
        
        if (this.i >= this.valueList.size()) {
            if (!endOfStreamEmitted) {
                endOfStreamEmitted = true;
                System.out.println("publisher_spout: reached end of input, emitting end_of_stream signal");
                this.collector.emit(new Values("broker1", -1L, new byte[0], true));
            }
            return;
        }
        String targetBroker = "broker" + ((publicationSeq % BROKER_COUNT) + 1);
        PublicationOuterClass.Publication basePublication = this.valueList.get(i++);
        PublicationOuterClass.Publication publication = PublicationOuterClass.Publication.newBuilder(basePublication)
                .setPubId(String.valueOf(publicationSeq++))
                .build();
        long timestamp = Instant.now().toEpochMilli();
            System.out.println("publisher_spout: emitting publication pubId=" + publication.getPubId()
                    + " (" + publicationSeq + "/" + this.valueList.size() + ") to " + targetBroker);
        this.collector.emit(new Values(targetBroker, timestamp, publication.toByteArray(), false));
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("target_broker", "timestamp", "publication_data", "end_of_stream"));
    }

}