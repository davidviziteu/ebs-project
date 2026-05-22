import java.util.ArrayList;
import java.util.HashMap;

class PublicationData
{
    String stationid;
    String city;
    String temp;
    String wind;
}

class SubscriptionData
{
    String operator;
    String value;
}

public class InputData
{
    ArrayList<PublicationData> publications;
    ArrayList<HashMap<String, SubscriptionData>> subscriptions;
}
