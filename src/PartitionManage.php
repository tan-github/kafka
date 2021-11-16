<?php
/**
 * 分区关联
 * User: Administrator
 * Date: 2021/11/12
 * Time: 16:32
 */

namespace Kafka;


class PartitionManage
{
    //redis对象
    private $redis_obj = null;
    //记录分区数量redis key
    private $redis_key = '';
    //分区数量
    private $part_num = 2;
    //分区数组是否存在
    private $is_part_arr_exist = false;
    //是否显示消息
    private $is_show = true;

    /**
     * @param int $part_num
     */
    public function setPartNum($part_num)
    {
        $this->part_num = $part_num;
        $this->showMsg('分区数组个数设置为：' . $part_num);
    }



    public function __construct($redis_obj,$redis_key)
    {
        $this->redis_obj = $redis_obj;
        $this->redis_key = $redis_key;
    }


    /**
     * 最小值分区
     * @param string $message
     */
    public function getMinNumPartiton()
    {
        $part_no = $this->getMinNumByPart();

        $this->showMsg('发送到的分区是：'.$part_no);

        $bool = $this->redis_obj->hIncrBy($this->redis_key, $part_no, 1);

        if ($bool !== false) {
            $this->showMsg('发送分区：（' . $part_no . '）数据更新成功');
        }else{
            $this->showMsg('发送分区：（' . $part_no . '）数据更新失败');
        }

        return $part_no;
    }

    /**
     * 消费设置分区数据
     * @param int $part_no
     */
    public function setPartitionNum($part_no = 0)
    {

        if (!$this->is_part_arr_exist) {
            $this->initPartitionNumArr();

        }


        $bool = $this->redis_obj->hIncrBy($this->redis_key, $part_no, -1);

        if ($bool !== false) {
            $this->showMsg('消费分区：（' . $part_no . '）数据更新成功');
        }else{
            $this->showMsg('消费分区：（' . $part_no . '）数据更新失败');
        }
    }

    /**
     * @return bool
     */
    public function isPartArrExist()
    {
        return $this->is_part_arr_exist;
    }


    /**
     * 初始化分区
     */
    private function initPartitionNumArr()
    {
        $bool = true;

        $part_no_arr = range(0, $this->part_num - 1, 1);

        $partition_num_arr = $this->redis_obj->hMget($this->redis_key, $part_no_arr);

        if ($partition_num_arr) {
            foreach ($partition_num_arr as $key => $value) {
                if ($value === false) {
                    $bool = $this->redis_obj->hset($this->redis_key, $key, 0);
                    if (!$bool) {
                        break;
                    }
                }
            }
        }

        if ($bool) {
            $this->is_part_arr_exist = true;
            $this->showMsg('初始化分区数据：{'.json_encode($partition_num_arr).'}');
        }
    }

    /**
     * 获取数值最小的分区
     * @return int|string
     */
    private function getMinNumByPart()
    {
        $rs_part_no = RD_KAFKA_PARTITION_UA;

        $partition_num_arr = $this->redis_obj->hGetAll($this->redis_key);
        var_dump($partition_num_arr);
        if ($partition_num_arr) {
            $init_num = false;

            foreach ($partition_num_arr as $part_no => $num) {
                if ($init_num === false) {
                    $init_num = $num;
                }else{
                    $num = (int)$num;
                    if ($num <= $init_num) {
                        $init_num = $num;
                        $rs_part_no = $part_no;
                    }
                }

                //小于0赋值为0
                if ($num < 0) {
                    $this->redis_obj->hSet($this->redis_key, $part_no, 0);
                }
            }
        }

        return $rs_part_no;

    }

    /**
     * 显示信息
     * @param $msg
     */
    private function showMsg($msg)
    {
        if ($this->is_show) {
            echo $msg . PHP_EOL;
        }
    }
}






