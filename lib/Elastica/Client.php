<?php

namespace Elastica;

use Elastica\Bulk\Action;
use Elastica\Bulk\ResponseSet;
use Elastica\Connection\Strategy\StrategyInterface;
use Elastica\Exception\ConnectionException;
use Elastica\Exception\InvalidException;
use Elastica\Script\AbstractScript;
use Elasticsearch\Endpoints\AbstractEndpoint;
use Elasticsearch\Endpoints\Indices\ForceMerge;
use Elasticsearch\Endpoints\Indices\Refresh;
use Elasticsearch\Endpoints\Update;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Client to connect the the elasticsearch server.
 *
 * @author Nicolas Ruflin <spam@ruflin.com>
 */
class Client
{
    /**
     * Config with defaults.
     *
     * log: Set to true, to enable logging, set a string to log to a specific file
     * retryOnConflict: Use in \Elastica\Client::updateDocument
     * bigintConversion: Set to true to enable the JSON bigint to string conversion option (see issue #717)
     *
     * @var array
     */
    protected $_config = [
        'host' => null,
        'port' => null,
        'path' => null,
        'url' => null,
        'proxy' => null,
        'transport' => null,
        'persistent' => true,
        'timeout' => null,
        'connections' => [], // host, port, path, timeout, transport, compression, persistent, timeout, username, password, config -> (curl, headers, url)
        'roundRobin' => false,
        'log' => false,
        'retryOnConflict' => 0,
        'bigintConversion' => false,
        'username' => null,
        'password' => null,
    ];

    /**
     * @var callable|null
     */
    protected $_callback;

    /**
     * @var Connection\ConnectionPool
     */
    protected $_connectionPool;

    /**
     * @var Request|null
     */
    protected $_lastRequest;

    /**
     * @var Response|null
     */
    protected $_lastResponse;

    /**
     * @var LoggerInterface
     */
    protected $_logger;

    /**
     * @var string
     */
    protected $_version;

    /**
     * Creates a new Elastica client.
     *
     * @param array           $config   OPTIONAL Additional config options
     * @param callable        $callback OPTIONAL Callback function which can be used to be notified about errors (for example connection down)
     * @param LoggerInterface $logger
     */
    public function __construct(array $config = [], callable $callback = null, LoggerInterface $logger = null)
    {
        $this->_callback = $callback;

        if (!$logger && isset($config['log']) && $config['log']) {
            $logger = new Log($config['log']);
        }
        $this->_logger = $logger ?: new NullLogger();

        $this->setConfig($config);
        $this->_initConnections();
    }

    /**
     * Get current version.
     *
     * @return string
     */
    public function getVersion(): string
    {
        if ($this->_version) {
            return $this->_version;
        }

        $data = $this->request('/')->getData();

        return $this->_version = $data['version']['number'];
    }

    /**
     * Inits the client connections.
     */
    protected function _initConnections()
    {
        $connections = [];

        foreach ($this->getConfig('connections') as $connection) {
            $connections[] = Connection::create($this->_prepareConnectionParams($connection));
        }

        if (isset($this->_config['servers'])) {
            foreach ($this->getConfig('servers') as $server) {
                $connections[] = Connection::create($this->_prepareConnectionParams($server));
            }
        }

        // If no connections set, create default connection
        if (empty($connections)) {
            $connections[] = Connection::create($this->_prepareConnectionParams($this->getConfig()));
        }

        if (!isset($this->_config['connectionStrategy'])) {
            if (true === $this->getConfig('roundRobin')) {
                $this->setConfigValue('connectionStrategy', 'RoundRobin');
            } else {
                $this->setConfigValue('connectionStrategy', 'Simple');
            }
        }

        $strategy = Connection\Strategy\StrategyFactory::create($this->getConfig('connectionStrategy'));

        $this->_connectionPool = new Connection\ConnectionPool($connections, $strategy, $this->_callback);
    }

    /**
     * Creates a Connection params array from a Client or server config array.
     *
     * @param array $config
     *
     * @return array
     */
    protected function _prepareConnectionParams(array $config): array
    {
        $params = [];
        $params['config'] = [];
        foreach ($config as $key => $value) {
            if (in_array($key, ['bigintConversion', 'curl', 'headers', 'url'])) {
                $params['config'][$key] = $value;
            } else {
                $params[$key] = $value;
            }
        }

        return $params;
    }

    /**
     * Sets specific config values (updates and keeps default values).
     *
     * @param array $config Params
     *
     * @return $this
     */
    public function setConfig(array $config): self
    {
        foreach ($config as $key => $value) {
            $this->_config[$key] = $value;
        }

        return $this;
    }

    /**
     * Returns a specific config key or the whole
     * config array if not set.
     *
     * @param string $key Config key
     *
     * @throws InvalidException
     *
     * @return array|string Config value
     */
    public function getConfig(string $key = '')
    {
        if (empty($key)) {
            return $this->_config;
        }

        if (!array_key_exists($key, $this->_config)) {
            throw new InvalidException('Config key is not set: '.$key);
        }

        return $this->_config[$key];
    }

    /**
     * Sets / overwrites a specific config value.
     *
     * @param string $key   Key to set
     * @param mixed  $value Value
     *
     * @return $this
     */
    public function setConfigValue(string $key, $value)
    {
        return $this->setConfig([$key => $value]);
    }

    /**
     * @param array|string $keys    config key or path of config keys
     * @param mixed        $default default value will be returned if key was not found
     *
     * @return mixed
     */
    public function getConfigValue($keys, $default = null)
    {
        $value = $this->_config;
        foreach ((array) $keys as $key) {
            if (isset($value[$key])) {
                $value = $value[$key];
            } else {
                return $default;
            }
        }

        return $value;
    }

    /**
     * Returns the index for the given connection.
     *
     * @param string $name Index name to create connection to
     *
     * @return Index Index for the given name
     */
    public function getIndex(string $name): Index
    {
        return new Index($this, $name);
    }

    /**
     * Adds a HTTP Header.
     *
     * @param string $header      The HTTP Header
     * @param string $headerValue The HTTP Header Value
     *
     * @return $this
     */
    public function addHeader(string $header, string $headerValue): self
    {
        $this->_config['headers'][$header] = $headerValue;

        return $this;
    }

    /**
     * Remove a HTTP Header.
     *
     * @param string $header The HTTP Header to remove
     *
     * @return $this
     */
    public function removeHeader(string $header): self
    {
        if (array_key_exists($header, $this->_config['headers'])) {
            unset($this->_config['headers'][$header]);
        }

        return $this;
    }

    /**
     * Uses _bulk to send documents to the server.
     *
     * Array of \Elastica\Document as input. Index and type has to be
     * set inside the document, because for bulk settings documents,
     * documents can belong to any type and index
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param array|Document[] $docs          Array of Elastica\Document
     * @param array            $requestParams
     *
     * @throws InvalidException If docs is empty
     *
     * @return ResponseSet Response object
     */
    public function updateDocuments(array $docs, array $requestParams = []): ResponseSet
    {
        if (empty($docs)) {
            throw new InvalidException('Array has to consist of at least one element');
        }

        $bulk = new Bulk($this);

        $bulk->addDocuments($docs, Action::OP_TYPE_UPDATE);
        foreach ($requestParams as $key => $value) {
            $bulk->setRequestParam($key, $value);
        }

        return $bulk->send();
    }

    /**
     * Uses _bulk to send documents to the server.
     *
     * Array of \Elastica\Document as input. Index and type has to be
     * set inside the document, because for bulk settings documents,
     * documents can belong to any type and index
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param array|Document[] $docs          Array of Elastica\Document
     * @param array            $requestParams
     *
     * @throws InvalidException If docs is empty
     *
     * @return ResponseSet Response object
     */
    public function addDocuments(array $docs, array $requestParams = []): ResponseSet
    {
        if (empty($docs)) {
            throw new InvalidException('Array has to consist of at least one element');
        }

        $bulk = new Bulk($this);

        $bulk->addDocuments($docs);

        foreach ($requestParams as $key => $value) {
            $bulk->setRequestParam($key, $value);
        }

        return $bulk->send();
    }

    /**
     * Update document, using update script. Requires elasticsearch >= 0.19.0.
     *
     * @param int|string                    $id      document id
     * @param array|AbstractScript|Document $data    raw data for request body
     * @param string                        $index   index to update
     * @param string                        $type    type of index to update
     * @param array                         $options array of query params to use for query. For possible options check es api
     *
     * @return Response
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
     */
    public function updateDocument($id, $data, string $index, string $type, array $options = []): Response
    {
        $endpoint = new Update();
        $endpoint->setID($id);
        $endpoint->setIndex($index);
        $endpoint->setType($type);

        if ($data instanceof AbstractScript) {
            $requestData = $data->toArray();
        } elseif ($data instanceof Document) {
            $requestData = ['doc' => $data->getData()];

            if ($data->getDocAsUpsert()) {
                $requestData['doc_as_upsert'] = true;
            }

            $docOptions = $data->getOptions([
                'version',
                'version_type',
                'routing',
                'percolate',
                'parent',
                'fields',
                'retry_on_conflict',
                'consistency',
                'replication',
                'refresh',
                'timeout',
            ]);
            $options += $docOptions;

            // set fields param to source only if options was not set before
            if ($data instanceof Document && ($data->isAutoPopulate()
                || $this->getConfigValue(['document', 'autoPopulate'], false))
                && !isset($options['fields'])
            ) {
                $options['fields'] = '_source';
            }
        } else {
            $requestData = $data;
        }

        //If an upsert document exists
        if ($data instanceof AbstractScript || $data instanceof Document) {
            if ($data->hasUpsert()) {
                $requestData['upsert'] = $data->getUpsert()->getData();
            }
        }

        if (!isset($options['retry_on_conflict'])) {
            if ($retryOnConflict = $this->getConfig('retryOnConflict')) {
                $options['retry_on_conflict'] = $retryOnConflict;
            }
        }

        $endpoint->setBody($requestData);
        $endpoint->setParams($options);

        $response = $this->requestEndpoint($endpoint);

        if (
            $data instanceof Document && $response->isOk() &&
            ($data->isAutoPopulate() || $this->getConfigValue(['document', 'autoPopulate'], false))
        ) {
            $responseData = $response->getData();
            if (isset($responseData['_version'])) {
                $data->setVersion($responseData['_version']);
            }
            if (isset($options['fields'])) {
                $this->_populateDocumentFieldsFromResponse($response, $data, $options['fields']);
            }
        }

        return $response;
    }

    /**
     * @param Response $response
     * @param Document $document
     * @param string   $fields   Array of field names to be populated or '_source' if whole document data should be updated
     */
    protected function _populateDocumentFieldsFromResponse(Response $response, Document $document, string $fields)
    {
        $responseData = $response->getData();
        if ('_source' === $fields) {
            if (isset($responseData['get']['_source']) && is_array($responseData['get']['_source'])) {
                $document->setData($responseData['get']['_source']);
            }
        } else {
            $keys = explode(',', $fields);
            $data = $document->getData();

            foreach ($keys as $key) {
                if (isset($responseData['get']['fields'][$key])) {
                    $data[$key] = $responseData['get']['fields'][$key];
                } elseif (isset($data[$key])) {
                    unset($data[$key]);
                }
            }

            $document->setData($data);
        }
    }

    /**
     * Bulk deletes documents.
     *
     * @param array|Document[] $docs
     * @param array            $requestParams
     *
     * @throws InvalidException
     *
     * @return ResponseSet
     */
    public function deleteDocuments(array $docs, array $requestParams = []): ResponseSet
    {
        if (empty($docs)) {
            throw new InvalidException('Array has to consist of at least one element');
        }

        $bulk = new Bulk($this);
        $bulk->addDocuments($docs, Action::OP_TYPE_DELETE);

        foreach ($requestParams as $key => $value) {
            $bulk->setRequestParam($key, $value);
        }

        return $bulk->send();
    }

    /**
     * Returns the status object for all indices.
     *
     * @return Status Status object
     */
    public function getStatus(): Status
    {
        return new Status($this);
    }

    /**
     * Returns the current cluster.
     *
     * @return Cluster Cluster object
     */
    public function getCluster(): Cluster
    {
        return new Cluster($this);
    }

    /**
     * Establishes the client connections.
     */
    public function connect()
    {
        $this->_initConnections();
    }

    /**
     * @param Connection $connection
     *
     * @return $this
     */
    public function addConnection(Connection $connection): self
    {
        $this->_connectionPool->addConnection($connection);

        return $this;
    }

    /**
     * Determines whether a valid connection is available for use.
     *
     * @return bool
     */
    public function hasConnection(): bool
    {
        return $this->_connectionPool->hasConnection();
    }

    /**
     * @throws ClientException
     *
     * @return Connection
     */
    public function getConnection(): Connection
    {
        return $this->_connectionPool->getConnection();
    }

    /**
     * @return Connection[]
     */
    public function getConnections(): array
    {
        return $this->_connectionPool->getConnections();
    }

    /**
     * @return StrategyInterface
     */
    public function getConnectionStrategy(): StrategyInterface
    {
        return $this->_connectionPool->getStrategy();
    }

    /**
     * @param Connection[] $connections
     *
     * @return $this
     */
    public function setConnections(array $connections): self
    {
        $this->_connectionPool->setConnections($connections);

        return $this;
    }

    /**
     * Deletes documents with the given ids, index, type from the index.
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param array        $ids     Document ids
     * @param string|Index $index   Index name
     * @param string|Type  $type    Type of documents
     * @param string|bool  $routing Optional routing key for all ids
     *
     * @throws InvalidException
     *
     * @return ResponseSet Response  object
     */
    public function deleteIds(array $ids, $index, $type, $routing = false): ResponseSet
    {
        if (empty($ids)) {
            throw new InvalidException('Array has to consist of at least one id');
        }

        $bulk = new Bulk($this);
        $bulk->setIndex($index);
        $bulk->setType($type);

        foreach ($ids as $id) {
            $action = new Action(Action::OP_TYPE_DELETE);
            $action->setId($id);

            if (!empty($routing)) {
                $action->setRouting($routing);
            }

            $bulk->addAction($action);
        }

        return $bulk->send();
    }

    /**
     * Bulk operation.
     *
     * Every entry in the params array has to exactly on array
     * of the bulk operation. An example param array would be:
     *
     * array(
     *         array('index' => array('_index' => 'test', '_type' => 'user', '_id' => '1')),
     *         array('user' => array('name' => 'hans')),
     *         array('delete' => array('_index' => 'test', '_type' => 'user', '_id' => '2'))
     * );
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @param array $params Parameter array
     *
     * @throws ResponseException
     * @throws InvalidException
     *
     * @return ResponseSet Response object
     */
    public function bulk(array $params): ResponseSet
    {
        if (empty($params)) {
            throw new InvalidException('Array has to consist of at least one param');
        }

        $bulk = new Bulk($this);

        $bulk->addRawData($params);

        return $bulk->send();
    }

    /**
     * Makes calls to the elasticsearch server based on this index.
     *
     * It's possible to make any REST query directly over this method
     *
     * @param string       $path        Path to call
     * @param string       $method      Rest method to use (GET, POST, DELETE, PUT)
     * @param array|string $data        OPTIONAL Arguments as array or pre-encoded string
     * @param array        $query       OPTIONAL Query params
     * @param string       $contentType Content-Type sent with this request
     *
     * @throws Exception\ConnectionException|Exception\ClientException
     *
     * @return Response Response object
     */
    public function request(
        string $path,
        string $method = Request::GET,
        $data = [],
        array $query = [],
        string $contentType = Request::DEFAULT_CONTENT_TYPE
    ): Response {
        $connection = $this->getConnection();
        $request = $this->_lastRequest = new Request($path, $method, $data, $query, $connection, $contentType);
        $this->_lastResponse = null;

        try {
            $response = $this->_lastResponse = $request->send();
        } catch (ConnectionException $e) {
            $this->_connectionPool->onFail($connection, $e, $this);

            $this->_log($e);

            // In case there is no valid connection left, throw exception which caused the disabling of the connection.
            if (!$this->hasConnection()) {
                throw $e;
            }

            return $this->request($path, $method, $data, $query);
        }

        $this->_log($request);

        return $response;
    }

    /**
     * Makes calls to the elasticsearch server with usage official client Endpoint.
     *
     * @param AbstractEndpoint $endpoint
     *
     * @return Response
     */
    public function requestEndpoint(AbstractEndpoint $endpoint): Response
    {
        return $this->request(
            ltrim($endpoint->getURI(), '/'),
            $endpoint->getMethod(),
            null === $endpoint->getBody() ? [] : $endpoint->getBody(),
            $endpoint->getParams()
        );
    }

    /**
     * logging.
     *
     * @deprecated Overwriting Client->_log is deprecated. Handle logging functionality by using a custom LoggerInterface.
     *
     * @param mixed $context
     */
    protected function _log($context)
    {
        if ($context instanceof ConnectionException) {
            $this->_logger->error('Elastica Request Failure', [
                'exception' => $context,
                'request' => $context->getRequest()->toArray(),
                'retry' => $this->hasConnection(),
            ]);

            return;
        }

        if ($context instanceof Request) {
            $this->_logger->debug('Elastica Request', [
                'request' => $context->toArray(),
                'response' => $this->_lastResponse ? $this->_lastResponse->getData() : null,
                'responseStatus' => $this->_lastResponse ? $this->_lastResponse->getStatus() : null,
            ]);

            return;
        }

        $this->_logger->debug('Elastica Request', [
            'message' => $context,
        ]);
    }

    /**
     * Optimizes all search indices.
     *
     * @param array $args OPTIONAL Optional arguments
     *
     * @return Response Response object
     *
     * @deprecated Replaced by forcemergeAll
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-optimize.html
     */
    public function optimizeAll(array $args = []): Response
    {
        trigger_error('Deprecated: Elastica\Client::optimizeAll() is deprecated and will be removed in further Elastica releases. Use Elastica\Client::forcemergeAll() instead.', E_USER_DEPRECATED);

        return $this->forcemergeAll($args);
    }

    /**
     * Force merges all search indices.
     *
     * @param array $args OPTIONAL Optional arguments
     *
     * @return Response Response object
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html
     */
    public function forcemergeAll(array $args = []): Response
    {
        $endpoint = new ForceMerge();
        $endpoint->setParams($args);

        return $this->requestEndpoint($endpoint);
    }

    /**
     * Refreshes all search indices.
     *
     * @return Response Response object
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html
     */
    public function refreshAll(): Response
    {
        return $this->requestEndpoint(new Refresh());
    }

    /**
     * @return Request|null
     */
    public function getLastRequest()
    {
        return $this->_lastRequest;
    }

    /**
     * @return Response|null
     */
    public function getLastResponse()
    {
        return $this->_lastResponse;
    }

    /**
     * Replace the existing logger.
     *
     * @param LoggerInterface $logger
     *
     * @return $this
     */
    public function setLogger(LoggerInterface $logger): self
    {
        $this->_logger = $logger;

        return $this;
    }
}
