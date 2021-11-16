<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2021/11/15
 * Time: 13:04
 */
require_once __DIR__.'/vendor/autoload.php';
date_default_timezone_set('PRC');

class TestPartition
{
    public $redis_obj = null;
    public $redis_key = '';
    public $part_manany = null;

    public function __construct()
    {
        $redis_obj = new \Redis();
        $redis_params = array(
            'hostname' => '192.168.1.122',
            'port' => '6379',
        );

        $flag = $redis_obj->connect($redis_params['hostname'],$redis_params['port']);
        $redis_obj->select(15);
        $this->redis_obj = $redis_obj;

        if(!$flag){
            throw new Exception("redis服务hostname:{$redis_params['hostname']},port:{$redis_params['port']}连接失败");
        }

        $this->redis_key = 'kafka_redis';

        $this->part_manany = new \Kafka\PartitionManage($redis_obj,$this->redis_key);
        $this->part_manany->setPartNum(5);
    }

    public function testSend()
    {
        $this->part_manany->getMinNumPartiton();

    }

    public function testCum()
    {
        $this->part_manany->setPartitionNum();

    }
}
$obj = new TestPartition();


//测试
if (count($argv) == 1) {
    echo 'send.....' . PHP_EOL;
    $obj->testSend();
}else{
    echo 'cum.....' . PHP_EOL;
    while (true) {
        $obj->testCum();
        sleep(5);
    }

}
