<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2021/5/27
 * Time: 10:03
 */

namespace Kafka;


class Example
{
    public function test()
    {
        $broker_list = "192.168.1.239:9092,192.168.1.247:9092,192.168.1.248:9092";
        $topic = 'test_php2';

        $rk_read = new Kafka(Kafka::CONSUMER,$broker_list,$topic);
        $rk_read->consumer(Example::class.'::msgCallback',Example::class.'::errorCallback',Example::class.'::listenCallback');

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

(new Example())->test();