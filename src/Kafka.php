<?php
namespace Kafka;

/**
 * Kafka消息队列
 * User: Administrator
 * Date: 2020/5/27 0027
 * Time: 15:32
 */

class Kafka
{
    const PRODUCER = 'producer';
    const CONSUMER = 'consumer';
    protected $broker_list = '';
    protected $topic = '';
    protected $partition = 0;
//    public $logFile = './kafkalog/info.log';     //计划在消费的时候写日志，指定日志文件

    protected $kafak_topic = null;
    public $producer = null;
    public $consumer = null;


    /**
     * kafka constructor.
     * @param $kafka_type 类型 'producer','consumer'
     * @param $broker_list
     * @param $topic
     * @param int $partition
     */
    public function __construct($kafka_type,$broker_list,$topic,$partition = 0,$consumer_group = 'my_consumer_group')
    {

        if (!in_array($kafka_type, array(self::PRODUCER, self::CONSUMER))) {
            echo '创建对象类型不正确'.PHP_EOL;
        }

        if (empty($broker_list))
        {
            echo 'broker_list 没有配置';
        }

        $this->broker_list = $broker_list;
        $this->topic = $topic;

        $this->partition = (int)$partition;

        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', $this->broker_list);

        //If you need to produce exactly once and want to keep the original produce order, uncomment the line below
        //$conf->set('enable.idempotence', 'true');
        if ($kafka_type == self::CONSUMER) {
            $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
//                        echo "Assign: ";
//                        var_dump($partitions);
                        $kafka->assign($partitions);
                        break;

                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
//                        echo "Revoke: ";
//                        var_dump($partitions);
                        $kafka->assign(NULL);
                        break;

                    default:
                        throw new \Exception($err);
                }
            });
            $conf->set('group.id', $consumer_group);
            $conf->set('auto.offset.reset', 'latest');
            $conf->set('enable.auto.commit', 'false');//关闭自动提交
            $conf->set('max.poll.interval.ms', '3600000');//超时时间1小时
//            $conf->set('session.timeout.ms', '10000');
//            $conf->set('heartbeat.interval.ms', '10000');
//            $conf->set('partition.assignment.strategy', 'roundrobin');//轮询读取分区
            $this->consumer = new \RdKafka\KafkaConsumer($conf);
            $this->consumer->subscribe([$this->topic]);
        }else{
            $this->producer = new \RdKafka\Producer($conf);
            $this->kafak_topic = $this->producer->newTopic($this->topic);
        }

    }

    /**
     * 生产者的方法
     * @param string $message
     */
    public function sendMessage($message = '')
    {
        $rs = array('status' => 0, 'msg' => '');

        $this->kafak_topic->produce(RD_KAFKA_PARTITION_UA, $this->partition, $message);
        $this->producer->poll(0);

        $result = $this->producer->flush(10000);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            $rs['msg'] = 'Was unable to flush, messages might be lost!';
        }else{
            $rs['msg'] = 'send successful!';
            $rs['status'] = 1;
        }

        return $rs;
    }

    /**
     * 消费者方法 （监听消息队列）
     * @throws Exception
     */
    public function consumer()
    {
        echo '开始监听.....' . PHP_EOL;
        while (true) {
            $message = $this->consumer->consume(120*1000);
            $error_msg = '';
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
//                    var_dump($message);
//                    $this->consumer->commit();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $error_msg = "No more messages; will wait for more";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $error_msg = "Timed out";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    $error_msg = $message->errstr();
                    break;
            }

            if ($error_msg) {
                echo $error_msg . PHP_EOL;
            }else{
                var_dump($message);
                $this->consumer->commit();
            }
        }
    }
}


//测试
//$broker_list = "192.168.1.239:9092,192.168.1.247:9092,192.168.1.248:9092";
//$topic = 'test_php2';

//消费者
//$rk = new Kafka(Kafka::CONSUMER,$broker_list,$topic,0);
//$rk->consumer();

//生成者
//$rk = new Kafka(Kafka::PRODUCER,$broker_list,$topic,0);
//$rs = $rk->sendMessage('aaa22221');
//var_dump($rs);