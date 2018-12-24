<?php

namespace Elastica;

use Elastica\Exception\InvalidException;
use Elastica\Query\AbstractQuery;
use Elastica\ResultSet\BuilderInterface;
use Elastica\ResultSet\DefaultBuilder;

/**
 * Elastica search object.
 *
 * @author   Nicolas Ruflin <spam@ruflin.com>
 */
class Search
{
    const OPTION_SEARCH_TYPE = 'search_type';
    const OPTION_ROUTING = 'routing';
    const OPTION_PREFERENCE = 'preference';
    const OPTION_VERSION = 'version';
    const OPTION_TIMEOUT = 'timeout';
    const OPTION_FROM = 'from';
    const OPTION_SIZE = 'size';
    const OPTION_SCROLL = 'scroll';
    const OPTION_SCROLL_ID = 'scroll_id';
    const OPTION_QUERY_CACHE = 'query_cache';
    const OPTION_TERMINATE_AFTER = 'terminate_after';
    const OPTION_SHARD_REQUEST_CACHE = 'request_cache';
    const OPTION_FILTER_PATH = 'filter_path';

    const OPTION_SEARCH_TYPE_DFS_QUERY_THEN_FETCH = 'dfs_query_then_fetch';
    const OPTION_SEARCH_TYPE_QUERY_THEN_FETCH = 'query_then_fetch';
    const OPTION_SEARCH_TYPE_SUGGEST = 'suggest';
    const OPTION_SEARCH_IGNORE_UNAVAILABLE = 'ignore_unavailable';

    const VALID_OPTIONS = [
        self::OPTION_SEARCH_TYPE,
        self::OPTION_ROUTING,
        self::OPTION_PREFERENCE,
        self::OPTION_VERSION,
        self::OPTION_TIMEOUT,
        self::OPTION_FROM,
        self::OPTION_SIZE,
        self::OPTION_SCROLL,
        self::OPTION_SCROLL_ID,
        self::OPTION_QUERY_CACHE,
        self::OPTION_TERMINATE_AFTER,
        self::OPTION_SHARD_REQUEST_CACHE,
        self::OPTION_FILTER_PATH,
        self::OPTION_SEARCH_TYPE_SUGGEST,
        self::OPTION_SEARCH_IGNORE_UNAVAILABLE,
    ];

    /**
     * @var BuilderInterface
     */
    private $_builder;

    /**
     * Array of indices.
     *
     * @var array
     */
    protected $_indices = [];

    /**
     * Array of types.
     *
     * @var array
     */
    protected $_types = [];

    /**
     * @var \Elastica\Query
     */
    protected $_query;

    /**
     * @var array
     */
    protected $_options = [];

    /**
     * Client object.
     *
     * @var Client
     */
    protected $_client;

    /**
     * Constructs search object.
     *
     * @param Client           $client  Client object
     * @param BuilderInterface $builder
     */
    public function __construct(Client $client, BuilderInterface $builder = null)
    {
        $this->_builder = $builder ?? new DefaultBuilder();
        $this->_client = $client;
    }

    /**
     * Adds a index to the list.
     *
     * @param Index|string $index Index object or string
     *
     * @throws InvalidException
     *
     * @return $this
     */
    public function addIndex($index): self
    {
        if ($index instanceof Index) {
            $index = $index->getName();
        }

        if (!is_string($index)) {
            throw new InvalidException('Invalid param type');
        }

        $this->_indices[] = $index;

        return $this;
    }

    /**
     * Add array of indices at once.
     *
     * @param array $indices
     *
     * @return $this
     */
    public function addIndices(array $indices = []): self
    {
        foreach ($indices as $index) {
            $this->addIndex($index);
        }

        return $this;
    }

    /**
     * Adds a type to the current search.
     *
     * @param Type|string $type Type name or object
     *
     * @throws InvalidException
     *
     * @return $this
     */
    public function addType($type): self
    {
        if ($type instanceof Type) {
            $type = $type->getName();
        }

        if (!is_string($type)) {
            throw new InvalidException('Invalid type type');
        }

        $this->_types[] = $type;

        return $this;
    }

    /**
     * Add array of types.
     *
     * @param array $types
     *
     * @return $this
     */
    public function addTypes(array $types = []): self
    {
        foreach ($types as $type) {
            $this->addType($type);
        }

        return $this;
    }

    /**
     * @param string|array|Query|Suggest|AbstractQuery $query
     *
     * @return $this
     */
    public function setQuery($query): self
    {
        $this->_query = Query::create($query);

        return $this;
    }

    /**
     * @param string $key
     * @param mixed  $value
     *
     * @return $this
     */
    public function setOption(string $key, $value): self
    {
        $this->_validateOption($key);

        $this->_options[$key] = $value;

        return $this;
    }

    /**
     * @param array $options
     *
     * @return $this
     */
    public function setOptions(array $options): self
    {
        $this->clearOptions();

        foreach ($options as $key => $value) {
            $this->setOption($key, $value);
        }

        return $this;
    }

    /**
     * @return $this
     */
    public function clearOptions(): self
    {
        $this->_options = [];

        return $this;
    }

    /**
     * @param string $key
     * @param mixed  $value
     *
     * @return $this
     */
    public function addOption(string $key, $value): self
    {
        $this->_validateOption($key);

        $this->_options[$key][] = $value;

        return $this;
    }

    /**
     * @param string $key
     *
     * @return bool
     */
    public function hasOption(string $key): bool
    {
        return isset($this->_options[$key]);
    }

    /**
     * @param string $key
     *
     * @throws InvalidException
     *
     * @return mixed
     */
    public function getOption($key)
    {
        if (!$this->hasOption($key)) {
            throw new InvalidException('Option '.$key.' does not exist');
        }

        return $this->_options[$key];
    }

    /**
     * @return array
     */
    public function getOptions(): array
    {
        return $this->_options;
    }

    /**
     * @param string $key
     *
     * @throws InvalidException
     *
     * @return bool
     */
    protected function _validateOption(string $key): bool
    {
        if (!\in_array($key, self::VALID_OPTIONS, true)) {
            throw new InvalidException("Invalid option $key");
        }

        return true;
    }

    /**
     * Return client object.
     *
     * @return Client Client object
     */
    public function getClient(): Client
    {
        return $this->_client;
    }

    /**
     * Return array of indices.
     *
     * @return string[] List of index names
     */
    public function getIndices(): array
    {
        return $this->_indices;
    }

    /**
     * @return bool
     */
    public function hasIndices(): bool
    {
        return count($this->_indices) > 0;
    }

    /**
     * @param Index|string $index
     *
     * @return bool
     */
    public function hasIndex($index): bool
    {
        if ($index instanceof Index) {
            $index = $index->getName();
        }

        return in_array($index, $this->_indices, true);
    }

    /**
     * Return array of types.
     *
     * @return string[] List of types
     */
    public function getTypes(): array
    {
        return $this->_types;
    }

    /**
     * @return bool
     */
    public function hasTypes(): bool
    {
        return count($this->_types) > 0;
    }

    /**
     * @param Type|string $type
     *
     * @return bool
     */
    public function hasType($type): bool
    {
        if ($type instanceof Type) {
            $type = $type->getName();
        }

        return in_array($type, $this->_types, true);
    }

    /**
     * @return Query
     */
    public function getQuery(): Query
    {
        if (null === $this->_query) {
            $this->_query = Query::create('');
        }

        return $this->_query;
    }

    /**
     * Creates new search object.
     *
     * @param SearchableInterface $searchObject
     *
     * @return Search
     */
    public static function create(SearchableInterface $searchObject): self
    {
        return $searchObject->createSearch();
    }

    /**
     * Combines indices and types to the search request path.
     *
     * @return string Search path
     */
    public function getPath(): string
    {
        if (isset($this->_options[self::OPTION_SCROLL_ID])) {
            return '_search/scroll';
        }

        $indices = $this->getIndices();

        $path = '';
        $types = $this->getTypes();

        if (empty($indices)) {
            if (!empty($types)) {
                $path .= '_all';
            }
        } else {
            $path .= implode(',', $indices);
        }

        if (!empty($types)) {
            $path .= '/'.implode(',', $types);
        }

        // Add full path based on indices and types -> could be all
        return $path.'/_search';
    }

    /**
     * Search in the set indices, types.
     *
     * @param mixed     $query
     * @param int|array $options OPTIONAL Limit or associative array of options (option=>value)
     *
     * @throws InvalidException
     *
     * @return ResultSet
     */
    public function search($query = '', $options = null): ResultSet
    {
        $this->setOptionsAndQuery($options, $query);

        $query = $this->getQuery();
        $path = $this->getPath();

        $params = $this->getOptions();

        // Send scroll_id via raw HTTP body to handle cases of very large (> 4kb) ids.
        if ('_search/scroll' === $path) {
            $data = [self::OPTION_SCROLL_ID => $params[self::OPTION_SCROLL_ID]];
            unset($params[self::OPTION_SCROLL_ID]);
        } else {
            $data = $query->toArray();
        }

        $response = $this->getClient()->request(
            $path,
            Request::GET,
            $data,
            $params
        );

        return $this->_builder->buildResultSet($response, $query);
    }

    /**
     * @param mixed $query
     * @param $fullResult (default = false) By default only the total hit count is returned. If set to true, the full ResultSet including aggregations is returned
     *
     * @return int|ResultSet
     */
    public function count($query = '', bool $fullResult = false)
    {
        $this->setOptionsAndQuery(null, $query);

        // Clone the object as we do not want to modify the original query.
        $query = clone $this->getQuery();
        $query->setSize(0);
        $path = $this->getPath();

        $response = $this->getClient()->request(
            $path,
            Request::GET,
            $query->toArray(),
            [self::OPTION_SEARCH_TYPE => self::OPTION_SEARCH_TYPE_QUERY_THEN_FETCH]
        );
        $resultSet = $this->_builder->buildResultSet($response, $query);

        return $fullResult ? $resultSet : $resultSet->getTotalHits();
    }

    /**
     * @param array|int          $options
     * @param string|array|Query $query
     *
     * @return $this
     */
    public function setOptionsAndQuery($options = null, $query = ''): self
    {
        if ('' !== $query) {
            $this->setQuery($query);
        }

        if (is_int($options)) {
            $this->getQuery()->setSize($options);
        } elseif (is_array($options)) {
            if (isset($options['limit'])) {
                $this->getQuery()->setSize($options['limit']);
                unset($options['limit']);
            }
            if (isset($options['explain'])) {
                $this->getQuery()->setExplain($options['explain']);
                unset($options['explain']);
            }
            $this->setOptions($options);
        }

        return $this;
    }

    /**
     * @param Suggest $suggest
     *
     * @return $this
     */
    public function setSuggest(Suggest $suggest): self
    {
        return $this->setOptionsAndQuery([self::OPTION_SEARCH_TYPE_SUGGEST => 'suggest'], $suggest);
    }

    /**
     * Returns the Scroll Iterator.
     *
     * @param string $expiryTime
     *
     * @return Scroll
     */
    public function scroll(string $expiryTime = '1m'): Scroll
    {
        return new Scroll($this, $expiryTime);
    }

    /**
     * @return BuilderInterface
     */
    public function getResultSetBuilder(): BuilderInterface
    {
        return $this->_builder;
    }
}
