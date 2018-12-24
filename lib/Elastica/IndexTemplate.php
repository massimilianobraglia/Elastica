<?php

namespace Elastica;

/**
 * Elastica index template object.
 *
 * @author Dmitry Balabka <dmitry.balabka@gmail.com>
 *
 * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html
 */
class IndexTemplate
{
    /**
     * Index template name.
     *
     * @var string Index pattern
     */
    protected $_name;

    /**
     * Client object.
     *
     * @var Client Client object
     */
    protected $_client;

    /**
     * Creates a new index template object.
     *
     * @param Client $client Client object
     * @param string $name   Index template name
     */
    public function __construct(Client $client, string $name)
    {
        $this->_client = $client;
        $this->_name = $name;
    }

    /**
     * Deletes the index template.
     *
     * @return Response Response object
     */
    public function delete(): Response
    {
        return $this->request(Request::DELETE);
    }

    /**
     * Creates a new index template with the given arguments.
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html
     *
     * @param array $args OPTIONAL Arguments to use
     *
     * @return Response
     */
    public function create(array $args = []): Response
    {
        return $this->request(Request::PUT, $args);
    }

    /**
     * Checks if the given index template is already created.
     *
     * @return bool True if index exists
     */
    public function exists(): bool
    {
        $response = $this->request(Request::HEAD);

        return 200 === $response->getStatus();
    }

    /**
     * Returns the index template name.
     *
     * @return string Index name
     */
    public function getName(): string
    {
        return $this->_name;
    }

    /**
     * Returns index template client.
     *
     * @return Client Index client object
     */
    public function getClient(): Client
    {
        return $this->_client;
    }

    /**
     * Makes calls to the elasticsearch server based on this index template name.
     *
     * @param string $method Rest method to use (GET, POST, DELETE, PUT)
     * @param array  $data   OPTIONAL Arguments as array
     *
     * @return \Elastica\Response Response object
     */
    public function request(string $method, array $data = []): Response
    {
        $path = '_template/'.$this->getName();

        return $this->getClient()->request($path, $method, $data);
    }
}
