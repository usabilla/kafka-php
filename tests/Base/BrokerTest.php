<?php
namespace KafkaTest\Base;

use Kafka\Broker;
use Kafka\Socket;
use Kafka\SocketSync;

class BrokerTest extends \PHPUnit\Framework\TestCase
{
    /**
     * setDown
     *
     * @access public
     * @return void
     */
    public function tearDown()
    {
        \Kafka\Broker::getInstance()->clear();
    }

    /**
     * testGroupBrokerId
     *
     * @access public
     * @return void
     */
    public function testGroupBrokerId()
    {
        $broker = \Kafka\Broker::getInstance();
        $broker->setGroupBrokerId(1);
        $this->assertEquals($broker->getGroupBrokerId(), 1);
    }

    /**
     * testData
     *
     * @access public
     * @return void
     */
    public function testData()
    {
        $broker = \Kafka\Broker::getInstance();
        $data   = [
            'brokers' => [
                [
                    'host' => '127.0.0.1',
                    'port' => '9092',
                    'nodeId' => '0',
                ],
                [
                    'host' => '127.0.0.1',
                    'port' => '9192',
                    'nodeId' => '1',
                ],
                [
                    'host' => '127.0.0.1',
                    'port' => '9292',
                    'nodeId' => '2',
                ],
            ],
            'topics' => [
                [
                    'topicName' => 'test',
                    'errorCode' => 0,
                    'partitions' => [
                        [
                            'partitionId' => 0,
                            'errorCode' => 0,
                            'leader' => 0,
                        ],
                        [
                            'partitionId' => 1,
                            'errorCode' => 0,
                            'leader' => 2,
                        ],
                    ],
                ],
                [
                    'topicName' => 'test1',
                    'errorCode' => 25,
                    'partitions' => [
                        [
                            'partitionId' => 0,
                            'errorCode' => 0,
                            'leader' => 0,
                        ],
                        [
                            'partitionId' => 1,
                            'errorCode' => 0,
                            'leader' => 2,
                        ],
                    ],
                ],
            ],
        ];
        $broker->setData($data['topics'], $data['brokers']);
        $brokers = [
            0 => '127.0.0.1:9092',
            1 => '127.0.0.1:9192',
            2 => '127.0.0.1:9292'
        ];
        $topics  = [
            'test' => [
                0 => 0,
                1 => 2,
            ]
        ];
        $this->assertEquals($brokers, $broker->getBrokers());
        $this->assertEquals($topics, $broker->getTopics());
    }

    /**
     * testGetConnect
     *
     * @access public
     * @return void
     */
    public function getConnect()
    {
        $broker = \Kafka\Broker::getInstance();
        $data   = [
            [
                'host' => '127.0.0.1',
                'port' => '9092',
                'nodeId' => '0',
            ],
            [
                'host' => '127.0.0.1',
                'port' => '9193',
                'nodeId' => '1',
            ],
            [
                'host' => '127.0.0.1',
                'port' => '9292',
                'nodeId' => '2',
            ],
        ];
        $broker->setData([], $data);

        $socket = $this->getMockBuilder(\Kafka\Socket::class)
            ->setConstructorArgs(['127.0.0.1', '9192', 'testing'])
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->setMethods(['connect', 'setOnReadable', 'close'])
            ->getMock();

        $result = $broker->getMetaConnect('1');
        $this->assertFalse($result);
    }

    public function testGetConnectSeparatesSyncAndAsyncSockets(): void
    {
        /** @var Broker|\PHPUnit_Framework_MockObject_MockObject $broker */
        $broker = $this->createPartialMock(Broker::class, ['getSocket']);

        $syncSocket = $this->createMock(SocketSync::class);
        $broker->expects($this->at(0))
               ->method('getSocket')
               ->with('127.0.0.1', '9092', true, 'dataSockets')
               ->willReturn($syncSocket);

        $asyncSocket = $this->createMock(Socket::class);
        $broker->expects($this->at(1))
               ->method('getSocket')
               ->with('127.0.0.1', '9092', false, 'dataSockets')
               ->willReturn($asyncSocket);

        $data = [
            [
                'host' => '127.0.0.1',
                'port' => '9092',
                'nodeId' => '0',
            ],
        ];
        $broker->setData([], $data);
        $broker->setProcess(function ($data) {
        });

        $actualSyncSocket = $broker->getConnect('127.0.0.1:9092', 'dataSockets', true);
        self::assertSame($syncSocket, $actualSyncSocket);

        $actualAsyncSocket = $broker->getConnect('127.0.0.1:9092', 'dataSockets', false);
        self::assertSame($asyncSocket, $actualAsyncSocket);
    }

    public function testClosesDataAndMetaSockets(): void
    {
        $syncSocket = $this->createMock(SocketSync::class);
        $syncSocket->expects($this->exactly(2))
                   ->method('close');
        $asyncSocket = $this->createMock(Socket::class);
        $asyncSocket->expects($this->exactly(2))
                    ->method('close');

        /** @var Broker|\PHPUnit_Framework_MockObject_MockObject $broker */
        $broker = $this->createPartialMock(Broker::class, ['getSocket']);
        $broker->method('getSocket')
               ->willReturnOnConsecutiveCalls($syncSocket, $asyncSocket, $syncSocket, $asyncSocket);

        $data = [
            [
                'host' => '127.0.0.1',
                'port' => '9092',
                'nodeId' => '0',
            ],
        ];
        $broker->setData([], $data);
        $broker->setProcess(function ($data) {
        });

        $broker->getConnect('127.0.0.1:9092', 'dataSockets', true);
        $broker->getConnect('127.0.0.1:9092', 'dataSockets', false);
        $broker->getConnect('127.0.0.1:9092', 'metaSockets', true);
        $broker->getConnect('127.0.0.1:9092', 'metaSockets', false);
        $broker->clear();
    }

    /**
     * testGetConnect
     *
     * @access public
     * @return void
     */
    public function testConnectRandFalse()
    {
        $broker = \Kafka\Broker::getInstance();

        $result = $broker->getRandConnect();
        $this->assertFalse($result);
    }

    /**
     * testGetSocket
     *
     * @access public
     * @return void
     */
    public function testGetSocketNotSetConfig()
    {
        $broker   = \Kafka\Broker::getInstance();
        $hostname = '127.0.0.1';
        $port     = '9092';
        $socket   = $broker->getSocket($hostname, $port, true, 'testing');

        $this->assertInstanceOf(\Kafka\SocketSync::class, $socket);
    }
}
