<?php
// +----------------------------------------------------------------------
// | 数据格式
// +----------------------------------------------------------------------
// | Author: Michael
// +----------------------------------------------------------------------
// | date: 2020-09-25
// +----------------------------------------------------------------------
namespace Es;
Trait DataFormate
{

    /**
     * 将数据输出，用于接口
     * @param $code 错误状态码 1是无错误，其它是有错误
     * @param string $msg 提示信息
     * @param array $data 返回数据
     * @param int $jsonType 是否json格式化
     */
    public function echoData($code, $msg = '', $data = [], $jsonType = 1, $ext = '')
    {
        $res = $this->backData($code, $msg, $data, $jsonType, $ext);
        if ($jsonType == 1) {
            header('Content-Type:application/json; charset=utf-8');
        }
        print_r($res);
        exit;
    }

    /**
     * 数据返回格式，系统内部使用
     * @param $code 错误状态码 1是无错误，其它是有错误
     * @param string $msg 提示信息
     * @param array $data 返回数据
     * @param int $jsonType 是否json格式化
     * @return array
     */
    public function backData($code, $msg = '', $data = [], $jsonType = 1, $ext = '')
    {
        if (empty($msg)) {
            $msg = $code == 1 ? 'success' : 'fail';
        }
        $rData = $this->formateData($code, $msg, $data, $ext);
        if ($jsonType == 1) {
            $rData = json_encode($rData, JSON_UNESCAPED_UNICODE);
        }
        return $rData;
    }

    /**
     * 返回固定格式数据
     * @param $code 错误状态码 1是无错误，其它是有错误
     * @param string $msg 提示信息
     * @param array $data 返回数据
     * @param array|string $ext 扩展
     * @return array
     */
    public function formateData($code, $msg = '', $data = [], $ext = '')
    {
        if (empty($msg)) {
            $msg = $code == 1 ? 'success' : 'fail';
        }
        $returnData = [];

        $returnData['code'] = $code;
        $returnData['msg']  = $msg;
        $returnData['data'] = $data;
        $returnData['ext']  = $ext;

        return $returnData;
    }

    /**
     * 判断字符串是否是json格式
     * @param $string 需要判断的字符串
     * @return bool
     */
    public function isJson($string)
    {
        if (is_string($string) && !empty($string)) {
            @json_decode($string);
            return (json_last_error() === JSON_ERROR_NONE);
        }
        return false;
    }

}
