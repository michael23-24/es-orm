<?php
// +----------------------------------------------------------------------
// | elasticsearch 简单的 orm
// +----------------------------------------------------------------------
// | Copyright (c) 义幻科技 http://www.mobimedical.cn All rights reserved.
// +----------------------------------------------------------------------
// | Author: Michael
// +----------------------------------------------------------------------
// | date: 2020-09-17
// +----------------------------------------------------------------------
namespace EsOrm;

class EsOrm
{
    use DataFormate;
    /**
     * elasticsearch客户端对象
     * @var null
     */
    protected $esClient = null;

    /**
     * 查询条件
     * @var array
     */
    protected $query = [];

    /**
     * 聚合查询条件term
     * @var array
     */
    protected $aggsTerm = [];

    /**
     * 最近一次查询条件
     * @var array
     */
    public $lastQuery = [];

    /**
     * 索引
     * @var string
     */
    public $index = null;

    /**
     * 索引前缀
     * @var string
     */
    public $indexPrefix = null;

    /**
     * 支持的布尔查询类型
     * @var array
     */
    protected $boolType = ['must', 'must_not', 'should'];

    /**
     * 聚合查询名称后缀
     * @var int
     */
    protected $aggsSuffix = 0;

    /**
     * 聚合查询名称
     * @var string
     */
    protected $aggsName = 'aggsField_';

    /**
     * 聚合查询子桶名称
     * @var string
     */
    protected $aggsNameSub = 'aggsSubField_';

    /**
     * 聚合查询结构
     * @var array
     */
    protected $aggsStruct = [];


    /**
     * 实例化子类，并且返回对象
     * @return $this
     */
    public static function getInst($config)
    {
        static $ormObj = null;
        if ($ormObj == null) {
            $runClassName = get_called_class();
            $ormObj       = new $runClassName($config);
        }
        return $ormObj;
    }

    /**
     * orm constructor.
     * @param string $config
     */
    public function __construct($config)
    {
        if(empty($config)){
            throw new \Exception('配置不能为空');
        }

        $this->esClient = new EsClient($config);
    }

    public function __call($name, $arguments)
    {
        /**
         * 可直接访问es客户端
         */
        if (method_exists($this->esClient, $name)) {
            return call_user_func_array([$this->esClient, $name], $arguments);
        }
    }

    public function where($key, $value, $compare = "=", $bool = 'must', $filter = null, $params = [])
    {
        if (!in_array($bool, $this->boolType)) {
            throw new \Exception('无效的布尔类型');
        }
        $compare = trim($compare);
        switch ($compare) {
            case '=':
            default:
                $tool = "term";
                break;

            case 'range':
                $tool = "range";
                break;

            case 'exists':
                $tool = "exists";
                break;

            case 'match_phrase':
                $tool = "match_phrase";
                break;
        }

        switch ($tool) {
            case 'term':
            default:
                return $this->term($key, $value, $bool, $filter, $params);
            case 'range':
                return $this->range($key, $value, $bool, $filter, $params);
            case 'exists':
                return $this->exists($key, $value, $bool, $filter, $params);
            case 'match_phrase':
                return $this->matchPhrase($key, $value, $bool, $filter, $params);
        }

    }

    /**
     * matchPhrase
     * @param $key
     * @param $value
     * @param $bool
     * @param $filter
     * @param $params
     * @return $this
     */
    public function matchPhrase($key, $value, $bool, $filter, $params)
    {
        $qeury = [];
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $termQuery = ['match_phrase' => [$k => $v]];
                $qeury[]   = $termQuery;
            }
        } else {
            $termQuery = [
                $termQuery = ['match_phrase' => [$key => $value]]
            ];
            $qeury[]   = $termQuery;
        }
        if (empty($this->query['query']['bool'][$bool])) $this->query['query']['bool'][$bool] = [];
        $this->query['query']['bool'][$bool] = array_merge($this->query['query']['bool'][$bool], $qeury);

        return $this;
    }

    /**
     * term
     * @param $key
     * @param $value
     * @param $bool
     * @param $filter
     * @param $params
     * @return $this
     */
    public function term($key, $value, $bool, $filter, $params)
    {
        $qeury = [];
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                if (is_array($v)) {
                    $termQuery = ['terms' => [$k => $v]];
                } else {
                    $termQuery = ['term' => [$k => ['value' => $v]]];
                }

                $qeury[] = $termQuery;
            }
        } else {
            if (is_array($value)) {
                $termQuery = [
                    'terms' => [$key => $value]
                ];
            } else {
                $termQuery = [
                    'term' => [$key => ['value' => $value]]
                ];
            }
            $qeury[] = $termQuery;
        }
        if (empty($this->query['query']['bool'][$bool])) $this->query['query']['bool'][$bool] = [];
        $this->query['query']['bool'][$bool] = array_merge($this->query['query']['bool'][$bool], $qeury);

        return $this;
    }

    /**
     * @param array $key
     * @param $value
     * @param $bool
     * @param $filter
     * @param $params
     * @return $this
     */
    public function range(array $key, $value, $bool, $filter, $params)
    {
        $rangeQuery = [
            ['range' => $key]
        ];
        if (empty($this->query['query']['bool'][$bool])) $this->query['query']['bool'][$bool] = [];
        $this->query['query']['bool'][$bool] = array_merge($this->query['query']['bool'][$bool], $rangeQuery);
        return $this;
    }

    /**
     * term
     * @param $key
     * @param $value
     * @param $bool
     * @param $filter
     * @param $params
     * @return $this
     */
    public function exists($key, $value, $bool, $filter, $params)
    {
        $qeury = [];
        if (is_array($key)) {
            foreach ($key as $v) {
                $termQuery = ['exists' => ['field' => $v]];
                $qeury[]   = $termQuery;
            }
        } else {
            $termQuery = [
                'exists' => ['field' => $key]
            ];
            $qeury[]   = $termQuery;
        }
        if (empty($this->query['query']['bool'][$bool])) $this->query['query']['bool'][$bool] = [];
        $this->query['query']['bool'][$bool] = array_merge($this->query['query']['bool'][$bool], $qeury);

        return $this;
    }

    public function order($by, $direction = "asc", $override = false)
    {
        if ($override) {
            $this->query['sort'] = [];
        }
        if (is_array($by)) {
            foreach ($by as $k => $v) {
                $this->query['sort'][] = [$k => $v];
            }
        } else {
            $this->query['sort'][] = [$by => $direction];
        }
        return $this;
    }

    public function getLastQuery()
    {
        return $this->lastQuery;
    }

    /**
     * 构建搜索条件
     * @return array
     * @throws \Exception
     */
    public function buildQuery()
    {
        if (empty($this->index)) {
            throw new \Exception('没有设置index');
        }
        if (isset($this->query['query']['bool']['should'])) {
            $this->query['query']['bool']['minimum_should_match'] = 1;
        }
        if (!empty($this->aggsTerm)) {
            $boolParams = $this->query['query']['bool'];
            unset($this->query['query']);
            $this->aggsStruct;
            $aggsName                       = $this->getAggsName();
            $aggsStruct                     = [
                $aggsName => $this->aggsStruct
            ];
            $this->aggsStruct               = $aggsStruct;
            $this->query['aggs'][$aggsName] = [
                'filter' => ['bool' => $boolParams],
                'aggs'   => $this->aggsTerm,
            ];
        }
        $this->query['track_total_hits'] = true;
        $query           = [
            'index' => $this->getIndex(),
            'type'  => '_doc',
            'body'  => $this->query
        ];
        $this->lastQuery = $query;
        $this->query     = [];
        $this->aggsTerm  = [];
        return $query;
    }

    /**
     * 返回行数
     * @param $from 开始行数
     * @param $size 结束行数
     * @return $this
     */
    public function limit($from, $size)
    {
        if ($size) {
            $this->query['from'] = $from;
            $this->query['size'] = $size;
        } else {
            $this->query['from'] = 0;
            $this->query['size'] = $from;
        }
        return $this;
    }

    /**
     * 获取当前索引
     * @return string
     */
    public function getIndex()
    {
        return $this->indexPrefix . $this->index;
    }

    /**
     * 创建索引
     * @return array
     */
    public function create()
    {
        $params          = [
            'index' => $this->getIndex(),
        ];
        $this->lastQuery = $params;
        return $this->esClient->create($params);
    }

    /**
     * 索引文档
     * @param $body 设置文档字段
     * @param null $id 文档id
     * @return array
     */
    public function index($body, $id = null)
    {
        $params = [
            'index' => $this->getIndex(),
            'type'  => '_doc',
            'body'  => $body
        ];
        if ($id) {
            $params['id'] = $id;
        }
        $this->lastQuery = $params;
        return $this->esClient->index($params);
    }

    /**
     * 单一索引文档
     * @param $id
     * @return array
     */
    public function get($id)
    {
        $params          = [
            'index' => $this->getIndex(),
            'type'  => '_doc',
            'id'    => $id
        ];
        $this->lastQuery = $params;
        return $this->esClient->get($params);
    }

    /**
     * 更新文档
     * @param $id 文档id
     * @param array $field 需要更新的字段
     * @param array $other 其他更新，如upsert,script
     * @return array
     */
    public function update($id, array $field = [], array $other = [])
    {
        if (empty($field) && empty($field)) {
            return $this->formateData(0, '更新字段不能为空');
        }
        $params = [
            'index' => $this->getIndex(),
            'type'  => '_doc',
            'id'    => $id
        ];
        $body   = [];
        if (!empty($field)) {
            $body['doc'] = $field;
        }
        if (!empty($other)) {
            $body = array_merge($body, $other);
        }
        $params['body']  = $body;
        $this->lastQuery = $params;
        return $this->esClient->update($params);
    }

    /**
     * 删除文档
     * @param $id 文档id
     * @return array
     */
    public function delete($id)
    {
        $params          = [
            'index' => $this->getIndex(),
            'type'  => '_doc',
            'id'    => $id
        ];
        $this->lastQuery = $params;
        return $this->esClient->delete($params);
    }

    /**
     * 获取指定字段
     * @param $includes 包含字段
     * @param $excludes 排除字段
     * @return $this
     */
    public function field($includes, $excludes)
    {
        if (is_string($includes)) {
            $includes = explode(',', $includes);
        }
        if (is_string($excludes)) {
            $excludes = explode(',', $excludes);
        }
        if (!empty($includes)) {
            $this->query['_source']['includes'] = $includes;
        }
        if (!empty($excludes)) {
            $this->query['_source']['excludes'] = $excludes;
        }
        return $this;
    }

    /**
     * 分组查询
     * @param $field 分组字段
     * @return $this
     */
    public function groupby($field)
    {
        if (empty($field)) {
            return $this;
        }
        $aggsName                  = $this->getAggsName(true);
        $this->aggsStruct          = [
            $aggsName => []
        ];
        $this->aggsTerm[$aggsName] = ['terms' => ['field' => $field, 'size' => 10000]];
        $this->query['size']       = 0;
        return $this;
    }

    /**
     * 获取结合名称
     * @param bool $sub
     * @return string
     */
    protected function getAggsName($sub = false)
    {
        $this->aggsSuffix++;
        if ($sub == true) {
            return $this->aggsNameSub . $this->aggsSuffix;
        } else {
            return $this->aggsName . $this->aggsSuffix;
        }
    }

    /**
     * 搜索
     * @return array
     * @throws \Exception
     */
    public function search()
    {
        $indexQuery = $this->buildQuery();
        $response   = $this->esClient->search($indexQuery);
        if (!empty($this->aggsStruct)) {
            $data = [];
            foreach ($this->aggsStruct as $key => $value) {
                $data = $response['ext']['aggregations'][$key] ?: [];
                if (is_array($value)) {
                    foreach ($value as $k => $v) {
                        if ($data[$k]) {
                            $data = $data[$k];
                        }
                    }
                }
            }
            if (!empty($data)) {
                $response['data'] = $data['buckets'];
            }
            $this->aggsStruct = [];
        }
        return $response;
    }


}
