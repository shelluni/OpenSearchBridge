<?php
namespace Topxia\Service\OpenSearch;

require_once dirname(__FILE__).'/Cloudsearch/CloudsearchClient.php';
require_once dirname(__FILE__).'/Cloudsearch/CloudsearchDoc.php';
require_once dirname(__FILE__).'/Cloudsearch/CloudsearchIndex.php';
require_once dirname(__FILE__).'/Cloudsearch/CloudsearchSearch.php';

abstract class OpenSearch
{
    protected $cfgName = '';    //config中OPEN_SEARCH的索引名，一般由子类定义

    protected $cfg = '';

    protected $table = 'main';   //该model对应的表名称，一般主表为main
    protected $pkName = 'uid';   //主键的名称
    protected $keysCount = 0;    //如果使用addSearchKeys()时，会记录下当时要搜索的keys数量
    protected $opt = array();    //透过方法传入的$opt，每次调用方法时会积累，最后在search()使用
    protected $effectCount = 0;

    public function __construct()
    {
        $this->_initialize();
    }

    protected function _initialize()
    {
        $this->openApp($this->cfgName);
    }

    protected function _beforeSave(&$rec) {}
    protected function _afterSearch(&$rec) {}

    protected function openApp($cfgName)
    {
        $cfgBucket = require(dirname(__FILE__).'/config.php');
        //$cfgBucket = C('OPEN_SEARCH');
        if(!isset($cfgBucket[$cfgName]))  return false;

        $this->cfg = $cfgBucket[$cfgName];
        return true;
    }

    public function getEffectCount()
    {
        return $this->effectCount;
    }

    public function addPair($pairString)
    {
        if(empty($this->opt['pair']))
        {
            $this->opt['pair'] = $pairString;
        }
        else
        {
            $this->opt['pair'] .= ',' . $pairString;
        }

        return $this;
    }

    public function addSort($sort)
    {
        $opt = $this->opt;
        if(isset($opt['sort']))
        {
            if(!is_array($opt['sort']))
            {
                $opt['sort'] = array($opt['sort']);
            }
        }
        else
        {
            $opt['sort'] = array();
        }

        $opt['sort'][] = $sort;

        $this->opt = $opt;

        return $this;
    }

    public function addSearchKeys(&$opt, $keyField = 'keys')
    {
        $this->keysCount = count($opt['keys']);
        if($this->keysCount == 0) return $this;

        if(isset($opt['sort']))
        {
            if(!is_array($opt['sort']))
            {
                $opt['sort'] = array($opt['sort']);
            }
        }
        else
        {
            $opt['sort'] = array();
        }

        //用kvpair传递的名称是in_keys，在$keyField字段中搜索，找到的赋值1，用sum加起来，不需要默认值，pair中只有key，最多输入100个key
        $opt['filter'] = 'int_tag_match("in_keys", ' . $keyField . ', 1, "sum", "false", "false", 100)>=1';
        $opt['sort'][] = '-int_tag_match("in_keys", ' . $keyField . ', 1, "sum", "false", "false", 100)';
        $opt['pair'] = 'in_keys:' . join(':', $opt['keys']);

        $this->keysCount = count($opt['keys']);

        $this->opt = $opt + $this->opt; //相加时，后面出现的key会被忽略
        return $this;
    }

    private function baseCUD($cmd, &$recs, $table = '')
    {
        $cmd = strtoupper($cmd);
        $opts = array(
            'host' => $this->cfg['host'],
            'debug' => true
        );

        if($table == '') $table = $this->table;

        $client = new \CloudsearchClient($this->cfg['accessKey'], $this->cfg['secret'], $opts, $this->cfg['keyType']);
        $doc_obj = new \CloudsearchDoc($this->cfg['appName'], $client);
         
        $docs_to_upload = array();
        $i = 0;
        $count = count($recs);
        foreach($recs as &$rec)
        {
            //如果回false情况下就略过这一笔，但$i++不能在if()里，否则可能造成最后一些没被更新
            if($this->_beforeSave($rec) !== false)
            {
                $item = array();
                $item['cmd'] = $cmd;
                $item["fields"] = $rec;

                $docs_to_upload[] = $item;
                $this->effectCount++;
            }

            $i++;
            //不论准备送上去的有多少笔，切成每50笔就更新一次，避免超过单次调用的大小限制
            if($i % 50 == 0 || $i == $count)
            {
                $result = $doc_obj->update($docs_to_upload, $table);    //实际add/update/remove都一样
                $result = json_decode($result, true);

                if($result['status'] == 'FAIL')
                {
                    //@TODO: 重新发送，还是失败怎么办？本次调用有成功有失败怎么办？
                    if($result['errors']['code'] == '3007')
                    {
                        //频率限制，小歇一会儿重新发送
                        sleep(1);
                        $result = $doc_obj->update($docs_to_upload, $table);    //实际add/update/remove都一样
                        $result = json_decode($result, true);
                    }
                }

                $docs_to_upload = array();
                flush();
            }
        }

        return $result;
    }

    public function addRec(&$rec)
    {
        return $this->baseCUD('add', array($rec));
    }

    public function addRecs(&$recs)
    {
        return $this->baseCUD('add', $recs);
    }

    public function deleteRec(&$rec)
    {
        $recs = array(&$rec);
        return $this->baseCUD('delete', $recs);
    }

    public function deleteRecs(&$recs)
    {
        return $this->baseCUD('delete', $recs);
    }

    public function saveRec(&$rec)
    {
        $recs = array(&$rec);
        return $this->baseCUD('update', $recs);
    }

    public function saveRecs(&$recs)
    {
        return $this->baseCUD('update', $recs);
    }

    public function findRec($uid = '')
    {
        $cmd = strtoupper($cmd);
        $opts = array(
            'host' => $this->cfg['host'],
            'debug' => true
        );

        $client = new \CloudsearchClient($this->cfg['accessKey'], $this->cfg['secret'], $opts, $this->cfg['keyType']);
        $doc_obj = new \CloudsearchDoc($this->cfg['appName'], $client);
        $result = $doc_obj->detail($uid);
        $result = json_decode($result, true);

        return $result;
    }

    public function distinct($distinctFieldName)
    {
        $this->opt['distinct'] = $distinctFieldName;

        //"dist_key:$distinctFieldName,dist_count:1,dist_times:1,reserved:false";

        //$pairString = "duniqfield:$distinctFieldName";
        //$this->addPair($pairString);

        return $this;
    }

    public function search($query, &$opt = array())
    {
        //将传入的参数与内部的$opt合并
        $opt = $opt + $this->opt;

        //如果没有$query也没有keys就不去搜索，避免数据被偷走
        $query = trim($query);
        if ($query == "")
        {
            if(!isset($opt['keys']) || empty($opt['keys']))
            {
                return array();
            }
        }

        $summary = isset($opt['summary']) ? $opt['summary'] : null;
        $opt['start'] = isset($opt['start']) ? $opt['start'] : 0;
        $opt['limit'] = isset($opt['limit']) ? $opt['limit'] : 10;

        // 实例化一个搜索类
        $opts = array(
            'host' => $this->cfg['host'],
            'debug' => true
        );
        $client = new \CloudsearchClient($this->cfg['accessKey'], $this->cfg['secret'], $opts, $this->cfg['keyType']);
        $search_obj = new \CloudsearchSearch($client);
        // 指定一个应用用于搜索
        $search_obj->addIndex($this->cfg['appName']);

        $search_obj->setQueryString($query);

        if(!empty($opt['first']))
        {
            $search_obj->setFirstFormulaName($opt['first']); //设定粗排表达式名称
        }

        if(!empty($opt['second']))
        {
            $search_obj->setFormulaName($opt['second']); //设定精排表达式名称
        }

        // 过滤条件
        if(!empty($opt['filter']))
        {
            if(!is_array($opt['filter']))
            {
                $opt['filter'] = array($opt['filter']);
            }

            foreach ($opt['filter'] as $filter)
            {
                //ex: 'status=1'
                $search_obj->addFilter($filter);
            }
        }

        // 设定搜索排序器
        if(!empty($opt['sort']))
        {
            if(!is_array($opt['sort']))
            {
                $opt['sort'] = array($opt['sort']);
            }

            foreach ($opt['sort'] as $sort)
            {
                //ex:"+txt_weight"，"-book_uid"
                $search_obj->addSort(substr($sort, 1), substr($sort, 0, 1));
            }
        }

        // 设定kv-pair，由于pair可能有kv或只有key的形式，因此不在此处理，由调用者自行组装格式
        if(!empty($opt['pair']))
        {
            $search_obj->setPair($opt['pair']);
        }

        // 设定distinct, 目前简化只处理类似db中的group by单一字段功能
        if(!empty($opt['distinct']))
        {
            $search_obj->addDistinct($opt['distinct'], 1, 1, 'false', '', 'true');
        }

        //截断飘红设置
        if(!empty($opt['summary']))
        {
            if(!is_array($opt['summary']))
            {
                $opt['summary'] = array($opt['summary']);
            }

            foreach($opt['summary'] as $summary)
            {
                $summary['len'] = $summary['len'] ? $summary['len']*2 : 200;    //1个汉字为2个字节，系统里常用的为utf8，1个汉字长度为1，延续习惯而转换
                $summary['element'] = $summary['element'] ?: 'em';

                $search_obj->addSummary($summary['fieldName'], $summary['len'], $summary['element']);
            }
        }

        // 指定返回的搜索结果的格式为fulljson，会比指定成json多得到一个sortExprValues数组，
        // sortExprValues数组数量就是addSort的数量，其值是addSort依序的值
        // 注意：使用fulljson与json回传的数组层次不同，要是调整setFormat()，要对应调整php
        $search_obj->setFormat("fulljson");

        // 设定搜索的参数
        $search_opts = array(
            'start' => $opt['start'], // 从第几个开始搜搜
            'hits' => $opt['limit']    // 一共搜多少个结果
        );

        // 执行搜索，获取搜索结果
        $json = $search_obj->search($search_opts);
        //dg($client->getRequest());
        $this->opt = array();   //清除内部opt，待下次重新开始

        // 将json类型字符串解码
        $search_result = json_decode($json, true);
        //dg($search_result);
        if ($search_result['status'] == "OK")
        {
            $opt['total'] = $search_result['result']['total'];
            $returnitems = array();
            $callback = isset($opt['callback']) ? $opt['callback']: null;//_afterSearch()

            foreach ($search_result['result']['items'] as $key => $ritem)
            {
                $ritem['fields']['sortValue'] = $ritem['sortExprValues'];
                $this->_afterSearch($ritem['fields']);

                if(! empty($callback))  $callback($ritem['fields']);

                $returnitems[] = $ritem['fields'];
            }
            return $returnitems;
        }
        else
        {
            return false;
        }
    }

}


