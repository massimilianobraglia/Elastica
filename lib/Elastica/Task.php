<?php

namespace Elastica;

use Elastica\Exception\InvalidException;
use Elasticsearch\Endpoints\Tasks\Cancel;
use Elasticsearch\Endpoints\Tasks\Get;

/**
 * Represents elasticsearch task.
 *
 * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html
 */
class Task extends Param
{
    const WAIT_FOR_COMPLETION = 'wait_for_completion';
    const WAIT_FOR_COMPLETION_FALSE = 'false';
    const WAIT_FOR_COMPLETION_TRUE = 'true';

    /**
     * Task id, e.g. in form of nodeNumber:taskId.
     *
     * @var string
     */
    protected $_id;

    /**
     * Contains all status infos.
     *
     * @var Response Response object
     */
    protected $_response;

    /**
     * Data.
     *
     * @var array Data
     */
    protected $_data;

    /**
     * Client object.
     *
     * @var Client Client object
     */
    protected $_client;

    public function __construct(Client $client, string $id)
    {
        $this->_client = $client;
        $this->_id = $id;
    }

    /**
     * Returns task id.
     *
     * @return string|int
     */
    public function getId()
    {
        return $this->_id;
    }

    /**
     * Returns task data.
     *
     * @return array Task data
     */
    public function getData(): array
    {
        if (null === $this->_data) {
            $this->refresh();
        }

        return $this->_data;
    }

    /**
     * Returns response object.
     *
     * @return \Elastica\Response
     */
    public function getResponse(): Response
    {
        if (null === $this->_response) {
            $this->refresh();
        }

        return $this->_response;
    }

    /**
     * Refresh task status.
     *
     * @param array $options Options for endpoint
     */
    public function refresh(array $options = [])
    {
        $endpoint = new Get();
        $endpoint->setTaskId($this->_id);
        $endpoint->setParams($options);

        $this->_response = $this->_client->requestEndpoint($endpoint);
        $this->_data = $this->getResponse()->getData();
    }

    /**
     * @return bool
     */
    public function isCompleted(): bool
    {
        $data = $this->getData();

        return true === $data['completed'];
    }

    public function cancel(): Response
    {
        if (empty($this->_id)) {
            throw new InvalidException('No task id given');
        }

        $endpoint = new Cancel();
        $endpoint->setTaskId($this->_id);

        return $this->_client->requestEndpoint($endpoint);
    }
}
