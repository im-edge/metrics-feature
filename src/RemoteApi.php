<?php

namespace IcingaMetrics;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use Exception;
use gipfl\Protocol\JsonRpc\Error;
use gipfl\Protocol\JsonRpc\Handler\FailingPacketHandler;
use gipfl\Protocol\JsonRpc\Handler\NamespacedPacketHandler;
use gipfl\Protocol\JsonRpc\JsonRpcConnection;
use gipfl\Protocol\NetString\StreamWrapper;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use gipfl\Socket\UnixSocketInspection;
use gipfl\Socket\UnixSocketPeer;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Socket\ConnectionInterface;
use function posix_getegid;

class RemoteApi implements EventEmitterInterface
{
    use EventEmitterTrait;

    protected LoggerInterface $logger;
    protected AsyncRrdtool $rrdtool;
    protected RrdCachedClient $rrdCached;
    protected ?ControlSocket $controlSocket = null;

    public function __construct(
        LoggerInterface $logger,
        AsyncRrdtool $rrdtool,
        RrdCachedClient $rrdCached
    ) {
        $this->logger = $logger;
        $this->rrdtool = $rrdtool;
        $this->rrdCached = $rrdCached;
    }

    public function run(string $socketPath)
    {
        $this->initializeControlSocket($socketPath);
    }

    protected function initializeControlSocket(string $path)
    {
        if (empty($path)) {
            throw new \InvalidArgumentException('Control socket path expected, got none');
        }
        $this->logger->info("[socket] launching control socket in $path");
        $socket = new ControlSocket($path);
        $socket->run();
        $this->addSocketEventHandlers($socket);
        $this->controlSocket = $socket;
    }

    protected function isAllowed(UnixSocketPeer $peer): bool
    {
        if ($peer->getUid() === 0) {
            return true;
        }
        $myGid = posix_getegid();
        $peerGid = $peer->getGid();
        // Hint: $myGid makes also part of id -G, this is the fast lane for those using
        //       php-fpm and the user icingaweb2 (with the very same main group as we have)
        if ($peerGid === $myGid) {
            return true;
        }

        $uid = $peer->getUid();
        return in_array($myGid, array_map('intval', explode(' ', `id -G $uid`)));
    }

    protected function addSocketEventHandlers(ControlSocket $socket)
    {
        $rrdHandler = new RpcNamespaceRrd($this->rrdtool, $this->rrdCached);
        $socket->on('connection', function (ConnectionInterface $connection) use ($rrdHandler) {
            $jsonRpc = new JsonRpcConnection(new StreamWrapper($connection));
            $jsonRpc->setLogger($this->logger);

            $peer = UnixSocketInspection::getPeer($connection);
            if (!$this->isAllowed($peer)) {
                $jsonRpc->setHandler(new FailingPacketHandler(new Error(Error::METHOD_NOT_FOUND, sprintf(
                    '%s is not allowed to control this socket',
                    $peer->getUsername()
                ))));
                Loop::get()->addTimer(10, function () use ($connection) {
                    $connection->close();
                });
                return;
            }

            $handler = new NamespacedPacketHandler();
            $handler->registerNamespace('rrd', $rrdHandler);
            $jsonRpc->setHandler($handler);
        });
        $socket->on('error', function (Exception $error) {
            // Connection error, Socket remains functional
            $this->logger->error($error->getMessage());
        });
    }
}
