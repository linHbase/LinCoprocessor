package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;*/

import java.io.File;
import java.io.OutputStream;

/**
 * Created by Administrator on 2018/3/9 0009.
 */
public class WordOut extends BaseBasicBolt {
    File file = new File("D:\\工作文件夹\\out.txt");
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
      //  System.out.println("getSourceGlobalStreamid: "+tuple.getSourceGlobalStreamid().get_componentId());
      //  System.out.println(tuple.getSourceStreamId()+" : getSourceStreamId");
        if(tuple.getSourceGlobalStreamid().get_componentId().equals("counter")) {
            System.out.println(tuple.getStringByField("word-counter") + ": " + tuple.getIntegerByField("sum"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("file-out"));
    }
}
