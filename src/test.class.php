<?php
// +----------------------------------------------------------------------
// | 注释
// +----------------------------------------------------------------------
// | Copyright (c) 义幻科技 http://www.mobimedical.cn All rights reserved.
// +----------------------------------------------------------------------
// | Author: Michael
// +----------------------------------------------------------------------
// | date: 2020-09-08
// +----------------------------------------------------------------------
namespace EsOrm;

class test
{
    public static function getInstance()
    {
        $args = func_get_args();
        return call_user_func_array([new self(), 'factory'], [$args]);
    }

    public function factory()
    {
        var_dump('123');
    }
}
