import backtype.storm.StormSubmitter;
import bolts.WordOut;

import spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;


public class TopologyMain {
	public static void main(String[] args)  {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("reader",new WordReader(),2);
		builder.setBolt("normalizer", new WordNormalizer(),2)
			.shuffleGrouping("reader");
		builder.setBolt("counter", new WordCounter(),1)
				.shuffleGrouping("normalizer");
		//	.fieldsGrouping("word-normalizer", new Fields("word"));
		builder.setBolt("out",new WordOut(),1).shuffleGrouping("counter");
        // Configuration
		Config config = new Config();
		// config.put("wordsFile", "D:\\words.txt");
		config.setDebug(true);
        //Topology run
		/*conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();*/
		try {
			if (args != null && args.length > 0) {
				config.setNumWorkers(1);
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());

			} else {
				// 这里是本地模式下运行的启动代码。
				config.setMaxTaskParallelism(1);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("simple", config, builder.createTopology());
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
