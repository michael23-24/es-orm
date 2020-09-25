<?php
// +----------------------------------------------------------------------
// | 注释
// +----------------------------------------------------------------------
// | Copyright (c) 义幻科技 http://www.mobimedical.cn All rights reserved.
// +----------------------------------------------------------------------
// | Author: Administrator
// +----------------------------------------------------------------------
// | date: 2020-09-24
// +----------------------------------------------------------------------
$autoloader = require_once dirname(__DIR__) . '/vendor/autoload.php';

require '../src/DataFormate.php';
require '../src/EsClient.php';
require '../src/EsOrm.php';
$config = require './es.php';
use \Es\EsOrm;
$esObj = EsOrm::getInst($config);
$esObj->index = 'test';
$esObj->indexPrefix = 'y_';
$response = $esObj->get(234);
print_r($response);


