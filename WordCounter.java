package bolts;

/*import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;*/

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCounter extends BaseBasicBolt {

	Integer id;
	String name;
	Map<String, Integer> counters;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	/**
	 * 准备工作，初始化字段
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("fa",new Fields("word-counter","sum"));
		declarer.declare(new Fields("word-counter","sum"));
		/*for(String key: counters.keySet()){
			System.out.println("key value is: "+key+" and value is:"+counters.get(key));
		}*/
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		/*for(Object ff : input.getValues()){
			System.out.println("input:  " +ff.toString());
		}*/
		String str = input.getStringByField("word-single");

		// System.out.println("这个值： "+str);
		/**
		 * 如果单词不存在，就创建该单词的键值对，如果存在
		 * 那么该单词对应的值将增加 1
		 */
		System.out.println(input.getSourceComponent()+" >>>>>>>");
		if(input.getSourceComponent().equals(Constants.COORDINATED_STREAM_ID)){
			System.out.println("if >>>>>>>");
			collector.emit(new Values(str, counters.get(str)));
		}
		if(!counters.keySet().contains(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str);
			c++;
			counters.put(str, c);
		}

		collector.emit(new Values(str, counters.get(str)));
		//collector.emit(counters.entrySet());
	}
}
