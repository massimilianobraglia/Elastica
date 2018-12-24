<?php

namespace Elastica;

use Elastica\Node\Info;
use Elastica\Node\Stats;

/**
 * Elastica cluster node object.
 *
 * @author Nicolas Ruflin <spam@ruflin.com>
 */
class Node
{
    /**
     * Client.
     *
     * @var Client
     */
    protected $_client;

    /**
     * @var string|int Unique node id
     */
    protected $_id;

    /**
     * Node name.
     *
     * @var string Node name
     */
    protected $_name;

    /**
     * Node stats.
     *
     * @var Stats|null Node Stats
     */
    protected $_stats;

    /**
     * Node info.
     *
     * @var Info|null Node info
     */
    protected $_info;

    /**
     * Create a new node object.
     *
     * @param string|int $id     Node id or name
     * @param Client     $client Node object
     */
    public function __construct($id, Client $client)
    {
        $this->_client = $client;
        $this->setId($id);
    }

    /**
     * @return string|int Unique node id. Can also be name if id not exists.
     */
    public function getId()
    {
        return $this->_id;
    }

    /**
     * @param string $id Node id
     *
     * @return $this Refreshed object
     */
    public function setId($id): self
    {
        $this->_id = $id;
        $this->refresh();

        return $this;
    }

    /**
     * Get the name of the node.
     *
     * @return string Node name
     */
    public function getName(): string
    {
        if (null === $this->_name) {
            $this->_name = $this->getInfo()->getName();
        }

        return $this->_name;
    }

    /**
     * Returns the current client object.
     *
     * @return Client Client
     */
    public function getClient(): Client
    {
        return $this->_client;
    }

    /**
     * Return stats object of the current node.
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html
     *
     * @return Stats Node stats
     */
    public function getStats(): Stats
    {
        if (null === $this->_stats) {
            $this->_stats = new Stats($this);
        }

        return $this->_stats;
    }

    /**
     * Return info object of the current node.
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-info.html
     *
     * @return Info Node info object
     */
    public function getInfo(): Info
    {
        if (null === $this->_info) {
            $this->_info = new Info($this);
        }

        return $this->_info;
    }

    /**
     * Refreshes all node information.
     *
     * This should be called after updating a node to refresh all information
     */
    public function refresh()
    {
        $this->_stats = null;
        $this->_info = null;
    }
}
