<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2021/11/15
 * Time: 15:58
 */
require_once __DIR__.'/vendor/autoload.php';
date_default_timezone_set('PRC');

class TestKafka
{
    public function test()
    {
        $broker_list = "192.168.1.239:9094,192.168.1.247:9094,192.168.1.248:9094";
        $topic = 'php-product-html-test';

        $rk_read = new \Kafka\Kafka(\Kafka\Kafka::CONSUMER,$broker_list,$topic);

        $redis_obj = (new \Kafka\KafkaRedis())->getRedisObj();
        $rk_read->setPartitionManageObj(new \Kafka\PartitionManage($redis_obj, 'kafka_redis'));
        $rk_read->setUsePartitionManage(true);
        $rk_read->consumer(TestKafka::class.'::msgCallback',TestKafka::class.'::errorCallback',TestKafka::class.'::listenCallback');

    }

    /**
     * 处理接收的消息
     * @param $massages 接收的消息
     */
    public static function msgCallback($massages)
    {
        var_dump($massages);
    }

    /**
     * 监听调用
     */
    public static function listenCallback()
    {
        echo '监听调用' . PHP_EOL;
    }

    /**
     * 处理错误信息
     * @param string $error_msg 错误信息
     */
    public static function errorCallback($error_msg = '')
    {
        var_dump($error_msg);
    }

}

(new TestKafka())->test();