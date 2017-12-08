<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka Broker info manager
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Broker
{
    use SingletonTrait;
    // {{{ consts
    // }}}
    // {{{ members

    private $groupBrokerId = null;

    private $topics = [];

    private $brokers = [];

    private $metaSockets = [];

    private $dataSockets = [];

    private $process;

    private $socket;

    // }}}
    // {{{ functions
    // {{{ public function setProcess()

    public function setProcess(\Closure $process)
    {
        $this->process = $process;
    }

    // }}}
    // {{{ public function setGroupBrokerId()

    public function setGroupBrokerId($brokerId)
    {
        $this->groupBrokerId = $brokerId;
    }

    // }}}
    // {{{ public function getGroupBrokerId()

    public function getGroupBrokerId()
    {
        return $this->groupBrokerId;
    }

    // }}}
    // {{{ public function setData()

    public function setData($topics, $brokersResult)
    {
        $brokers = [];
        foreach ($brokersResult as $value) {
            $key           = $value['nodeId'];
            $hostname      = $value['host'] . ':' . $value['port'];
            $brokers[$key] = $hostname;
        }

        $change = false;
        if (serialize($this->brokers) != serialize($brokers)) {
            $this->brokers = $brokers;
            $change        = true;
        }

        $newTopics = [];
        foreach ($topics as $topic) {
            if ($topic['errorCode'] != \Kafka\Protocol::NO_ERROR) {
                $this->error('Parse metadata for topic is error, error:' . \Kafka\Protocol::getError($topic['errorCode']));
                continue;
            }
            $item = [];
            foreach ($topic['partitions'] as $part) {
                $item[$part['partitionId']] = $part['leader'];
            }
            $newTopics[$topic['topicName']] = $item;
        }

        if (serialize($this->topics) != serialize($newTopics)) {
            $this->topics = $newTopics;
            $change       = true;
        }

        return $change;
    }

    // }}}
    // {{{ public function getTopics()

    public function getTopics()
    {
        return $this->topics;
    }

    // }}}
    // {{{ public function getBrokers()

    public function getBrokers()
    {
        return $this->brokers;
    }

    // }}}
    // {{{ public function getMetaConnect()

    public function getMetaConnect($key, $modeSync = false)
    {
        return $this->getConnect($key, 'metaSockets', $modeSync);
    }

    // }}}
    // {{{ public function getRandConnect()

    public function getRandConnect($modeSync = false)
    {
        $nodeIds = array_keys($this->brokers);
        shuffle($nodeIds);
        if (! isset($nodeIds[0])) {
            return false;
        }
        return $this->getMetaConnect($nodeIds[0], $modeSync);
    }

    // }}}
    // {{{ public function getDataConnect()

    public function getDataConnect($key, $modeSync = false)
    {
        return $this->getConnect($key, 'dataSockets', $modeSync);
    }

    // }}}
    // {{{ public function getConnect()

    public function getConnect($key, $type, $modeSync = false)
    {
        if (isset($this->{$type}[(int) $modeSync][$key])) {
            return $this->{$type}[(int) $modeSync][$key];
        }

        if (isset($this->brokers[$key])) {
            $hostname = $this->brokers[$key];
            if (isset($this->{$type}[(int) $modeSync][$hostname])) {
                return $this->{$type}[(int) $modeSync][$hostname];
            }
        }

        $host = null;
        $port = null;
        if (isset($this->brokers[$key])) {
            $hostname          = $this->brokers[$key];
            list($host, $port) = explode(':', $hostname);
        }

        if (strpos($key, ':') !== false) {
            list($host, $port) = explode(':', $key);
        }

        if (! $host || ! $port || (! $modeSync && ! $this->process)) {
            return false;
        }

        try {
            $socket = $this->getSocket($host, $port, $modeSync, $type);
            if (! $modeSync) {
                $socket->setOnReadable($this->process);
            }
            $socket->connect();
            $this->{$type}[(int) $modeSync][$key] = $socket;

            return $socket;
        } catch (\Exception $e) {
            $this->error($e->getMessage());

            return false;
        }
    }

    // }}}
    // {{{ public function clear()

    public function clear()
    {
        $closeSocket = function($socket) {
            $socket->close();
        };
        foreach($this->metaSockets as $type => $sockets) {
            array_walk($sockets, $closeSocket);
        }
        foreach ($this->dataSockets as $type => $sockets) {
            array_walk($sockets, $closeSocket);
        }
        $this->brokers = [];
    }

    // }}}
    // {{{ public function getSocket()

    public function getSocket($host, $port, $modeSync, $identifier)
    {
        if ($this->socket != null) {
            return $this->socket;
        }

        if ($modeSync) {
            $socket = new \Kafka\SocketSync($host, $port, $identifier);
        } else {
            $socket = new \Kafka\Socket($host, $port, $identifier);
        }
        return $socket;
    }

    // }}}
    // {{{ public function setSocket()

    // use unit test
    public function setSocket($socket)
    {
        $this->socket = $socket;
    }

    // }}}
    // }}}
}
