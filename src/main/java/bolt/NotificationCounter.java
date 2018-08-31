package bolt;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger ;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User:     Master King
 * Date:     2018/4/2
 * Desc:     apinotification
 * Version:  1.0
 **/

public class NotificationCounter extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    private static String delimiter="\t";
    /**
     * notification规则
     * @var group[0]   时间戳
     * @var group[1]   json
     */
    private String patter = "[^ ]*\\[(.*)\\](.*)___(\\{.+\\})(.*)";

    /**
     * @var date 日期
     * @var time 时间
     * @var channel
     * @var type 分类
     * @var taskId 任务id
     * @var tokenNum  token 总数量
     * @var numberSuccess   成功数
     * @var numberFailure   失败数
     * @var tokens  token数据
     * @var regData 正则匹配出的json初数据
     * @var errorMessage 错误数据
     */
    private String date, time, channel, type, taskId, tokenNum,
            numberSuccess, numberFailure, tokens, regData, regDate, errMsg;



    /**
     * json对象
     * @var jsonParser
     */
    private JsonParser jsonParser;

    /**
     * jsonParse后的对象
     * @var JsonObject
     */
    private JsonObject json;


    public void execute(Tuple input, BasicOutputCollector collector) {
        this.jsonParser = new JsonParser();
        this.errMsg = null ;

        // 获取数据
        String string = input.getString(0);

        // 正则匹配数据
        this.regMatch(string);


        String result = "error"+delimiter+string;
        // 数据拼接
        if (this.errMsg != "false") {
            result = "success"+ delimiter +
                    this.date + delimiter +
                    this.time + delimiter +
                    this.type + delimiter +
                    this.channel + delimiter +
                    this.tokenNum + delimiter +
                    this.numberSuccess + delimiter +
                    this.numberFailure + delimiter +
                    this.taskId + delimiter + this.tokens;
        }
        System.out.println(result);
        collector.emit(new Values(result));
    }

    /**
     * 提交数据时声明的字段
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("notification"));
    }


    public void cleanup() {
        System.out.println("finished");
    }


    /**
     * 正则匹配数据。group(1) 时间 group(3) json数据
     *
     * @param string
     */
    private void regMatch(String string) {
        Pattern pattern = Pattern.compile(this.patter);
        Matcher matcher = pattern.matcher(string);

        if (matcher.find()) {
            this.regData = matcher.group(3);
            this.regDate = matcher.group(1);
            // 处理时间
            this.timestamp();
            // 处理数据
            this.jsondata();
        } else {
            //this.errMsg = "error From regMath: match fail";
            this.errMsg = "false" ;
        }
    }


    private void jsondata() {
        try {
            this.json = (JsonObject) jsonParser.parse(this.regData);

            // tokens
            if (this.json.has("tokens")) {
                this.tokens = "";
                // 获取Json元素对象。
                JsonElement jsonElement = this.json.get("tokens");
                // 提取token
                if (jsonElement.isJsonArray()) {
                    this.getTokenFromJsonArr(jsonElement.getAsJsonArray());
                } else {
                    this.getTokenFromJsonStr(jsonElement.getAsString());
                }
            } else {
                this.tokens = "None";
            }

            // 计算tokens的总数
            this.tokenNum = this.tokens != "None" ? this.tokens.split(";").length + "" : "None";

            // 提取消息类型
            if (this.json.has("type")) {
                this.type = this.json.get("type").getAsString();
            } else {
                this.type = "None";
            }

            // 提取消息的channel
            if (this.json.has("channel")) {
                this.channel = this.json.get("channel").getAsString();
            } else {
                this.channel = "None";
            }

            // 提取numberSuccess和Failure数
            if (this.json.has("numberSuccess")) {
                this.numberSuccess = json.get("numberSuccess").getAsString();
            } else {
                this.numberSuccess = "0";
            }

            if (this.json.has("numberFailure")) {
                this.numberFailure = json.get("numberFailure").getAsString();
            } else {
                this.numberFailure = "0";
            }

            // 提取TaskId
            if (this.json.has("taskId")) {
                this.taskId = json.get("taskId").toString();
            } else {
                this.taskId = "None";
            }
        } catch (Exception e) {
            this.errMsg = "Error From jsonData:" + e.getMessage();
        }
    }

    /**
     * json获得是一个json。继续处理
     * @param jsonArray
     */
    private void getTokenFromJsonArr(JsonArray jsonArray) {
        Iterator iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            String tmp = iterator.next().toString();
            // 去掉前后的"
            if (tmp.startsWith("\"")) {
                tmp = tmp.substring(1);
            }
            if (tmp.endsWith("\"")) {
                tmp = tmp.substring(1,tmp.length()-1);
            }
            this.tokens += tmp+";";
        }
    }

    /**
     * json处理一个string。
     * @param jsonStr
     */
    private void getTokenFromJsonStr(String jsonStr) {
        String[] temp = jsonStr.split(",");
        for (String str: temp) {
            this.tokens += str + ";";
        }
    }

    /**
     * 采集这次任务的时间
     */
    private void timestamp() {
        String[] DateTime = this.regDate.split(" ");
        this.date = DateTime[0];
        this.time = DateTime[1];
    }
}