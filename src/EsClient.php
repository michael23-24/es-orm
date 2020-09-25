<?php
// +----------------------------------------------------------------------
// | 操作es底层
// +----------------------------------------------------------------------
// | Copyright (c) 义幻科技 http://www.mobimedical.cn All rights reserved.
// +----------------------------------------------------------------------
// | Author: Michael
// +----------------------------------------------------------------------
// | date: 2020-09-17
// +----------------------------------------------------------------------
namespace EsOrm;

class EsClient
{
    use DataFormate;

    /**
     * elasticsearch客户端对象
     * @var null
     */
    protected $esClient = null;

    public function __construct($config)
    {
        $this->esClient = \Elasticsearch\ClientBuilder::fromConfig($config, true);
    }

    /**
     * 创建索引
     * @param $params 索引参数
     * @return array
     */
    public function create($params){
        try {
            $respones = $this->esClient->indices()->create($params);
            return $this->formateData(1, 'ok', $respones);
        } catch (\Exception $e) {
            writeLogStruct("es_index", combinLog($e));
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}");
        }
    }

    /**
     * 索引文档
     * @param $params 索引参数
     * @return array
     */
    public function index($params)
    {
        try {
            $respones = $this->esClient->index($params);
            return $this->formateData(1, 'ok', $respones);
        } catch (\Exception $e) {
            writeLogStruct("es_index", combinLog($e));
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}");
        }
    }

    /**
     * 批量索引文档
     * @param $params 索引参数
     * @return array
     */
    public function bulk($params)
    {
        try {
            $respones = $this->esClient->bulk($params);
            return $this->formateData(1, 'ok', $respones);
        } catch (\Exception $e) {
            writeLogStruct("es_index", combinLog($e));
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}");
        }
    }

    /**
     * 单一索引文档
     * @param $params 索引参数
     * @return array
     */
    public function get($params)
    {
        try {
            $respones = $this->esClient->get($params);
            return $this->formateData(1, 'ok', $respones['_source']);
        } catch (\Exception $e) {
            $msgData = $this->isJson($e->getMessage()) ? json_decode($e->getMessage(), true) : [];
            if (isset($msgData['found']) && $msgData['found'] === false) {
                return $this->formateData(2, '没有查询到数据', $msgData);
            }
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}", $e);
        }
    }

    /**
     * 更新文档
     * @param $params 索引参数
     * @return array
     */
    public function update($params)
    {
        try {
            $respones = $this->esClient->update($params);
            return $this->formateData(1, 'ok', $respones);
        } catch (\Exception $e) {
            $msgData = $this->isJson($e->getMessage()) ? json_decode($e->getMessage(), true) : [];
            if (isset($msgData['error']) && isset($msgData['error']['reason'])) {
                return $this->formateData(2, "es抛出异常：{$msgData['error']['reason']}", $msgData);
            }
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}", $e);
        }
    }

    /**
     * 删除文档
     * @param $params 索引参数
     * @return array
     */
    public function delete($params)
    {
        try {
            $respones = $this->esClient->delete($params);
            return $this->formateData(1, 'ok', $respones);
        } catch (\Exception $e) {
            $msgData = $this->isJson($e->getMessage()) ? json_decode($e->getMessage(), true) : [];
            if (isset($msgData['result'])) {
                return $this->formateData(2, "es抛出异常：{$msgData['result']}", $msgData);
            }
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}", $e);
        }
    }

    /**
     * 搜索操作
     * @param $params 索引参数
     * @return array
     */
    public function search($params)
    {
        try {
            $respones   = $this->esClient->search($params);
            $returnData = [
                'total'  => $respones['hits']['total']['value'] ?: 0,
                'source' => $this->formateSource($respones)
            ];
            return $this->formateData(1, 'ok', $returnData,$respones);
        } catch (\Exception $e) {
            $msgData = $this->isJson($e->getMessage()) ? json_decode($e->getMessage(), true) : [];
            if (isset($msgData['error'])) {
                return $this->formateData(2, "es抛出异常：{$msgData['error']['reason']}", $msgData);
            }
            return $this->formateData(0, "es抛出异常：{$e->getMessage()}");
        }
    }

    /**
     * 格式化搜索的数据
     * @param $data 搜索源数据
     * @return array
     */
    protected function formateSource($data)
    {
        $source = [];
        if (isset($data['hits']['hits'])) {
            foreach ($data['hits']['hits'] as $value) {
                array_push($source, $value['_source']);
            }
        }
        return $source;
    }
}
