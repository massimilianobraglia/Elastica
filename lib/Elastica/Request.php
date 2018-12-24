<?php

namespace Elastica;

use Elastica\Exception\InvalidException;

/**
 * Elastica Request object.
 *
 * @author Nicolas Ruflin <spam@ruflin.com>
 */
class Request extends Param
{
    const HEAD = 'HEAD';
    const POST = 'POST';
    const PUT = 'PUT';
    const GET = 'GET';
    const DELETE = 'DELETE';
    const DEFAULT_CONTENT_TYPE = 'application/json';
    const NDJSON_CONTENT_TYPE = 'application/x-ndjson';

    /**
     * @var \Elastica\Connection
     */
    protected $_connection;

    /**
     * Construct.
     *
     * @param string       $path        Request path
     * @param string       $method      OPTIONAL Request method (use const's) (default = self::GET)
     * @param array|string $data        OPTIONAL Data array
     * @param array        $query       OPTIONAL Query params
     * @param Connection   $connection
     * @param string       $contentType Content-Type sent with this request
     */
    public function __construct(
        string $path,
        string $method = self::GET,
        $data = [],
        array $query = [],
        Connection $connection = null,
        string $contentType = self::DEFAULT_CONTENT_TYPE
    ) {
        $this->setPath($path);
        $this->setMethod($method);
        $this->setData($data);
        $this->setQuery($query);

        if ($connection) {
            $this->setConnection($connection);
        }
        $this->setContentType($contentType);
    }

    /**
     * Sets the request method. Use one of the for consts.
     *
     * @param string $method Request method
     *
     * @return $this
     */
    public function setMethod(string $method): self
    {
        return $this->setParam('method', $method);
    }

    /**
     * Get request method.
     *
     * @return string Request method
     */
    public function getMethod(): string
    {
        return $this->getParam('method');
    }

    /**
     * Sets the request data.
     *
     * @param array|string $data Request data
     *
     * @return $this
     */
    public function setData($data): self
    {
        return $this->setParam('data', $data);
    }

    /**
     * Return request data.
     *
     * @return array|string Request data
     */
    public function getData()
    {
        return $this->getParam('data');
    }

    /**
     * Sets the request path.
     *
     * @param string $path Request path
     *
     * @return $this
     */
    public function setPath(string $path): self
    {
        return $this->setParam('path', $path);
    }

    /**
     * Return request path.
     *
     * @return string Request path
     */
    public function getPath(): string
    {
        return $this->getParam('path');
    }

    /**
     * Return query params.
     *
     * @return array Query params
     */
    public function getQuery(): array
    {
        return $this->getParam('query');
    }

    /**
     * @param array $query
     *
     * @return $this
     */
    public function setQuery(array $query = []): self
    {
        return $this->setParam('query', $query);
    }

    /**
     * @param Connection $connection
     *
     * @return $this
     */
    public function setConnection(Connection $connection): self
    {
        $this->_connection = $connection;

        return $this;
    }

    /**
     * Return Connection Object.
     *
     * @throws InvalidException If no valid connection was setted
     *
     * @return Connection
     */
    public function getConnection(): Connection
    {
        if (null === $this->_connection) {
            throw new InvalidException('No valid connection object set');
        }

        return $this->_connection;
    }

    /**
     * Set the Content-Type of this request.
     *
     * @param string $contentType
     *
     * @return $this
     */
    public function setContentType(string $contentType): self
    {
        return $this->setParam('contentType', $contentType);
    }

    /**
     * Get the Content-Type of this request.
     */
    public function getContentType(): string
    {
        return $this->getParam('contentType');
    }

    /**
     * Sends request to server.
     *
     * @return Response Response object
     */
    public function send(): Response
    {
        $transport = $this->getConnection()->getTransportObject();

        // Refactor: Not full toArray needed in exec?
        return $transport->exec($this, $this->getConnection()->toArray());
    }

    /**
     *{@inheritdoc}
     */
    public function toArray()
    {
        $data = $this->getParams();
        if ($this->_connection) {
            $data['connection'] = $this->_connection->getParams();
        }

        return $data;
    }

    /**
     * Converts request to curl request format.
     *
     * @return string
     */
    public function toString(): string
    {
        return JSON::stringify($this->toArray());
    }

    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return $this->toString();
    }
}
