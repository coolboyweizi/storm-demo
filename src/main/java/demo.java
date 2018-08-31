import bolt.NotificationCounter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 * User:     Master King
 * Date:     2018/4/2
 * Desc:     apinotification
 * Version:  1.0
 **/

public class demo {

    // global
    private final static String KAFKA_SPOUT_ID = "notification_kafka_id" ;
    private final static String KAFKA_COUNT_ID = "notification_counter"  ;
    private final static String HDFS_STORE_ID  = "notification_hdfs_id"  ;
    private final static String TASK_NAME = "Singa";

    // kafka
    private final static int    zkPort  = 2181 ;
    private final static String zkId    = "a";
    private final static String zkHost  = "192.168.0.170";
    private final static String zkRoot  = "";
    private final static String zkTopic = "a_notification";


    // Hdfs
    private final static int        hdfsPort = 9000;
    private final static int        tupleNum = 1000 ;
    private final static String     hdfsHost = "192.168.0.170";
    private final static String     hdfsPref = "SingApiNotification";
    private final static String     hdfsPath = "/storm/SingApiNotification";
    private final static String     hdfsExts = ".log";
    private final static SyncPolicy sync = new CountSyncPolicy(tupleNum);
    private final static FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
    private final static RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(" : ");


    /**
     * hdfs 文件命名规范
     * @return FileNameFormat
     */
    private final static FileNameFormat getFileNameFormat() {
        return new FileNameFormat() {
            public void prepare(Map map, TopologyContext topologyContext) { }

            public String getPath() {
                return hdfsPath;
            }

            public String getName(long l, long timestamp) {
                Date date = new Date(timestamp);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddkkmmss");
                return hdfsPref+'_'+sdf.format(date)+hdfsExts;
            }
        };
    }

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // KafkaSpout
        BrokerHosts brokerHosts = new ZkHosts(zkHost+":"+zkPort);
        SpoutConfig spoutConfig = new SpoutConfig( brokerHosts  , zkTopic, zkRoot, zkId);

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkPort = zkPort ;
        spoutConfig.zkServers = Arrays.asList(new String[] {zkHost});
        spoutConfig.fetchSizeBytes = 10485760;

        // 测试忽略offset
        spoutConfig.ignoreZkOffsets=true;

        // HdfsBolt
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://"+hdfsHost+":"+hdfsPort)
                .withFileNameFormat(getFileNameFormat())
                .withSyncPolicy(sync)
                .withRotationPolicy(rotationPolicy)
                .withRecordFormat(format);

        // TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConfig));
        builder.setBolt(KAFKA_COUNT_ID, new NotificationCounter()).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(HDFS_STORE_ID, hdfsBolt).fieldsGrouping(KAFKA_COUNT_ID, new Fields("notification"));

        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            config.setMaxTaskParallelism(3);
            config.put(Config.NIMBUS_HOST, args[0]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(TASK_NAME, config, builder.createTopology());
        }else {
            LocalCluster cluster = new LocalCluster();
            config.setMaxTaskParallelism(3);
            cluster.submitTopology(TASK_NAME, config, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }


    }
}
