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

    //kafka服务地址
    private $broker_list = '';
    //kafka主题
    private $topic = '';
    //计划在消费的时候写日志，指定日志文件
    //public $logFile = './kafkalog/info.log';

    //是否轮询读取分区
    private $is_roundrobin = true;
    //生成者kafka主题对象
    private $kafak_topic = null;
    //生产者对象
    private $producer = null;
    //消费者对象
    private $consumer = null;

    //是否显示监听提升
    private $is_show_listens_tip = true;

    //超时时间(默认2分钟)
    private $timeout_ms = 120 * 1000;

    //心跳时间（毫秒）
    private $heartbeat_ms = 0;

    /**
     * 设置是否开启监听提升
     * @param bool $is_show_listens_tip
     */
    public function setIsShowListensTip(bool $is_show_listens_tip): void
    {
        $this->is_show_listens_tip = $is_show_listens_tip;
    }

    /**
     * 设置消费超时时间(毫秒)
     * @param float|int $timeout_ms
     */
    public function setTimeoutMs($timeout_ms): void
    {
        $this->timeout_ms = $timeout_ms;
    }

    /**
     * 获取生产者对象
     * @return null|\RdKafka\Producer
     */
    public function getProducer(): ?\RdKafka\Producer
    {
        return $this->producer;
    }

    /**
     * 获取消费者对象
     * @return null|\RdKafka\KafkaConsumer
     */
    public function getConsumer(): ?\RdKafka\KafkaConsumer
    {
        return $this->consumer;
    }

    
    /**
     * kafka constructor.
     * @param $kafka_type 类型 'producer','consumer'
     * @param $broker_list
     * @param $topic
     * @param int $partition
     */
    public function __construct($kafka_type,$broker_list,$topic,$consumer_group = 'my_consumer_group')
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
            //关闭自动提交
            $conf->set('enable.auto.commit', 'false');
            //超时时间1小时
            $conf->set('max.poll.interval.ms', '3600000');

            if ($this->heartbeat_ms > 500) {
                $conf->set('session.timeout.ms', $this->heartbeat_ms * 3);
                $conf->set('heartbeat.interval.ms', $this->heartbeat_ms);
            }

            if ($this->is_roundrobin) {
                $conf->set('partition.assignment.strategy', 'roundrobin');//轮询读取分区
            }
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

        $this->kafak_topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
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
    public function consumer($msg_callback = null,$error_callback = null,$listening_callback = null)
    {
        while (true) {
            $message = $this->consumer->consume($this->timeout_ms);

            $error_msg = '';
            //是否是监听中
            $is_listening = false;

            try {
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
//                    var_dump($message);
//                    $this->consumer->commit();
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        $error_msg = "No more messages; will wait for more";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        $is_listening = true;
                        $error_msg = "Timed out";
                        break;
                    default:
                        throw new \Exception($message->errstr(), $message->err);
                        $error_msg = $message->errstr();
                        break;
                }

                if ($error_msg) {
                    if (!$is_listening) {
                        if ($error_callback) {
                            call_user_func($error_callback, $error_msg);
                        }
                    }else{
                        if ($this->is_show_listens_tip) {
                            echo date('Y-m-d H:i:s').' 监听中.....' . PHP_EOL;
                        }
                        //监听调用
                        if ($listening_callback) {
                            call_user_func($listening_callback,$this);
                        }
                    }
                }else{
                    if (isset($message->payload) && $message->payload) {
                        echo '读取到分区（' . $message->partition . '）消息：【' . $message->payload . '】' . PHP_EOL;

                        if ($msg_callback) {
                            call_user_func($msg_callback,$message->payload);
                        }

                        $this->consumer->commit();
                    }
                }
            } catch (\Exception $ex) {
                if ($error_callback) {
                    call_user_func($error_callback, $ex->getMessage());
                }

                sleep(10);
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