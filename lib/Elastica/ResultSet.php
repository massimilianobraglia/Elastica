<?php

namespace Elastica;

use Elastica\Exception\InvalidException;

/**
 * Elastica result set.
 *
 * List of all hits that are returned for a search on elasticsearch
 * Result set implements iterator
 *
 * @author Nicolas Ruflin <spam@ruflin.com>
 */
class ResultSet implements \Iterator, \Countable, \ArrayAccess
{
    /**
     * Current position.
     *
     * @var int Current position
     */
    private $_position = 0;

    /**
     * Query.
     *
     * @var Query Query object
     */
    private $_query;

    /**
     * Response.
     *
     * @var Response Response object
     */
    private $_response;

    /**
     * Results.
     *
     * @var Result[] Results
     */
    private $_results;

    /**
     * Constructs ResultSet object.
     *
     * @param Response $response Response object
     * @param Query    $query    Query object
     * @param Result[] $results
     */
    public function __construct(Response $response, Query $query, array $results)
    {
        $this->_query = $query;
        $this->_response = $response;
        $this->_results = $results;
    }

    /**
     * Returns all results.
     *
     * @return Result[] Results
     */
    public function getResults(): array
    {
        return $this->_results;
    }

    /**
     * Returns all Documents.
     *
     * @return Document[]
     */
    public function getDocuments(): array
    {
        $documents = [];
        foreach ($this->_results as $doc) {
            $documents[] = $doc->getDocument();
        }

        return $documents;
    }

    /**
     * Returns true if the response contains suggestion results; false otherwise.
     *
     * @return bool
     */
    public function hasSuggests(): bool
    {
        $data = $this->_response->getData();

        return isset($data['suggest']);
    }

    /**
     * Return all suggests.
     *
     * @return array suggest results
     */
    public function getSuggests(): array
    {
        $data = $this->_response->getData();

        return $data['suggest'] ?? [];
    }

    /**
     * Returns whether aggregations exist.
     *
     * @return bool Aggregation existence
     */
    public function hasAggregations(): bool
    {
        $data = $this->_response->getData();

        return isset($data['aggregations']);
    }

    /**
     * Returns all aggregation results.
     *
     * @return array
     */
    public function getAggregations(): array
    {
        $data = $this->_response->getData();

        return $data['aggregations'] ?? [];
    }

    /**
     * Retrieve a specific aggregation from this result set.
     *
     * @param string $name the name of the desired aggregation
     *
     * @throws InvalidException if an aggregation by the given name cannot be found
     *
     * @return array
     */
    public function getAggregation(string $name): array
    {
        $data = $this->_response->getData();

        if (isset($data['aggregations']) && isset($data['aggregations'][$name])) {
            return $data['aggregations'][$name];
        }

        throw new InvalidException("This result set does not contain an aggregation named {$name}.");
    }

    /**
     * Returns the total number of found hits.
     *
     * @return int Total hits
     */
    public function getTotalHits(): int
    {
        $data = $this->_response->getData();

        return $data['hits']['total'] ?? 0;
    }

    /**
     * Returns the max score of the results found.
     *
     * @return float Max Score
     */
    public function getMaxScore(): float
    {
        $data = $this->_response->getData();

        return $data['hits']['max_score'] ?? 0.0;
    }

    /**
     * Returns the total number of ms for this search to complete.
     *
     * @return int Total time
     */
    public function getTotalTime(): int
    {
        $data = $this->_response->getData();

        return $data['took'] ?? 0;
    }

    /**
     * Returns true if the query has timed out.
     *
     * @return bool Timed out
     */
    public function hasTimedOut(): bool
    {
        $data = $this->_response->getData();

        return !empty($data['timed_out']);
    }

    /**
     * Returns response object.
     *
     * @return Response Response object
     */
    public function getResponse(): Response
    {
        return $this->_response;
    }

    /**
     * @return Query
     */
    public function getQuery(): Query
    {
        return $this->_query;
    }

    /**
     * Returns size of current set.
     *
     * @return int
     */
    public function count(): int
    {
        return count($this->_results);
    }

    /**
     * Returns size of current suggests.
     *
     * @return int Size of suggests
     */
    public function countSuggests(): int
    {
        return count($this->getSuggests());
    }

    /**
     * {@inheritdoc}
     */
    public function current(): Result
    {
        return $this->_results[$this->key()];
    }

    /**
     * {@inheritdoc}
     */
    public function next()
    {
        ++$this->_position;
    }

    /**
     * {@inheritdoc}
     */
    public function key(): int
    {
        return $this->_position;
    }

    /**
     * {@inheritdoc}
     */
    public function valid(): bool
    {
        return isset($this->_results[$this->key()]);
    }

    /**
     * {@inheritdoc}
     */
    public function rewind()
    {
        $this->_position = 0;
    }

    /**
     * {@inheritdoc}
     */
    public function offsetExists($offset): bool
    {
        return isset($this->_results[$offset]);
    }

    /**
     * {@inheritdoc}
     */
    public function offsetGet($offset): Result
    {
        if ($this->offsetExists($offset)) {
            return $this->_results[$offset];
        }

        throw new InvalidException('Offset does not exist.');
    }

    /**
     * {@inheritdoc}
     */
    public function offsetSet($offset, $value)
    {
        if (!($value instanceof Result)) {
            throw new InvalidException('ResultSet is a collection of Result only.');
        }

        if (!isset($this->_results[$offset])) {
            throw new InvalidException('Offset does not exist.');
        }

        $this->_results[$offset] = $value;
    }

    /**
     * {@inheritdoc}
     */
    public function offsetUnset($offset)
    {
        unset($this->_results[$offset]);
    }
}
