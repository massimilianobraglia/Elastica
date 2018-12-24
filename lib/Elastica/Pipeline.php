<?php

namespace Elastica;

use Elastica\Exception\InvalidException;
use Elastica\Exception\NotImplementedException;
use Elastica\Processor\AbstractProcessor;
use Elasticsearch\Endpoints\AbstractEndpoint;
use Elasticsearch\Endpoints\Ingest\Pipeline\Delete;
use Elasticsearch\Endpoints\Ingest\Pipeline\Get;
use Elasticsearch\Endpoints\Ingest\Pipeline\Put;

/**
 * Elastica Pipeline object.
 *
 * Handles Pipeline management & definition.
 *
 * @author   Federico Panini <fpanini@gmail.com>
 *
 * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-processors.html
 */
class Pipeline extends Param
{
    /**
     * @var string name of the pipeline
     */
    protected $id;

    /**
     * Client Object.
     *
     * @var Client Client object
     */
    protected $_client;

    /**
     * Processors array.
     *
     * @var array
     */
    protected $_processors = [];

    /**
     * Create a new Pipeline Object.
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->_client = $client;
    }

    /**
     * Create a Pipeline.
     *
     * @return Response
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html
     */
    public function create(): Response
    {
        if (null === $this->id) {
            throw new InvalidException('You should set a valid pipeline id');
        }

        if (empty($this->_params['description'])) {
            throw new InvalidException('You should set a valid processor description.');
        }

        if (empty($this->_processors['processors'])) {
            throw new InvalidException('You should set a valid processor of type Elastica\Processor\AbstractProcessor.');
        }

        $endpoint = new Put();
        $endpoint->setID($this->id);
        $endpoint->setBody($this->toArray());

        return $this->requestEndpoint($endpoint);
    }

    /**
     * Get a Pipeline Object.
     *
     * @param string $id Pipeline name
     *
     * @return Response
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/get-pipeline-api.html
     */
    public function getPipeline(string $id): Response
    {
        $endpoint = new Get();
        $endpoint->setID($id);

        return $this->requestEndpoint($endpoint);
    }

    /**
     * Delete a Pipeline.
     *
     * @param string $id Pipeline name
     *
     * @return Response
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html
     */
    public function deletePipeline(string $id): Response
    {
        $endpoint = new Delete();
        $endpoint->setID($id);

        return $this->requestEndpoint($endpoint);
    }

    /**
     * @todo implement simulate API
     */
    public function simulate()
    {
        throw new NotImplementedException('simulate API on Pipeline not yet implemented.');
    }

    /**
     * Sets query as raw array. Will overwrite all already set arguments.
     *
     * @param array $processors array
     *
     * @return $this
     */
    public function setRawProcessors(array $processors): self
    {
        $this->_processors = $processors;

        return $this;
    }

    /**
     * Add a processor.
     *
     * @param AbstractProcessor $processor
     *
     * @return $this
     */
    public function addProcessor(AbstractProcessor $processor): self
    {
        if (empty($this->_processors)) {
            $this->_processors['processors'] = $processor->toArray();
            $this->_params['processors'] = [];
        } else {
            $this->_processors['processors'] = array_merge($this->_processors['processors'], $processor->toArray());
        }

        return $this;
    }

    /**
     * Set pipeline id.
     *
     * @param string|int $id
     */
    public function setId($id)
    {
        $this->id = $id;
    }

    /**
     * Sets the processors.
     *
     * @param array $processors array of AbstractProcessor object
     *
     * @return $this
     */
    public function setProcessors(array $processors): self
    {
        return $this->setParam('processors', [$processors]);
    }

    /**
     * Set Description.
     *
     * @param string $description
     *
     * @return $this
     */
    public function setDescription(string $description): self
    {
        return $this->setParam('description', $description);
    }

    /**
     * {@inheritdoc}
     */
    public function toArray()
    {
        $this->_params['processors'] = [$this->_processors['processors']];

        return $this->getParams();
    }

    /**
     * Returns client.
     *
     * @return Client Client object
     */
    public function getClient(): Client
    {
        return $this->_client;
    }

    /**
     * Makes calls to the elasticsearch server with usage official client Endpoint based on this index.
     *
     * @param AbstractEndpoint $endpoint
     *
     * @return Response
     */
    public function requestEndpoint(AbstractEndpoint $endpoint): Response
    {
        $cloned = clone $endpoint;

        return $this->getClient()->requestEndpoint($cloned);
    }
}
