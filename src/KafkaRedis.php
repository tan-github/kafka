<?php
/**
 * 测试用
 * User: Administrator
 * Date: 2021/11/15
 * Time: 16:47
 */

namespace Kafka;


class KafkaRedis
{
    public function getRedisObj()
    {
        $redis_obj = new \Redis();
        $redis_params = array(
            'hostname' => '192.168.1.122',
            'port' => '6379',
        );

        $flag = $redis_obj->connect($redis_params['hostname'],$redis_params['port']);
        $redis_obj->select(15);


        if(!$flag){
            throw new Exception("redis服务hostname:{$redis_params['hostname']},port:{$redis_params['port']}连接失败");
        }

        return $redis_obj;
    }
}