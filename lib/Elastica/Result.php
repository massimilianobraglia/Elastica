<?php

namespace Elastica;

/**
 * Elastica result item.
 *
 * Stores all information from a result
 *
 * @author Nicolas Ruflin <spam@ruflin.com>
 */
class Result
{
    /**
     * Hit array.
     *
     * @var array Hit array
     */
    protected $_hit = [];

    /**
     * Constructs a single results object.
     *
     * @param array $hit Hit data
     */
    public function __construct(array $hit)
    {
        $this->_hit = $hit;
    }

    /**
     * Returns a param from the result hit array.
     *
     * This function can be used to retrieve all data for which a specific
     * function doesn't exist.
     * If the param does not exist, an empty array is returned
     *
     * @param string $name Param name
     *
     * @return mixed Result data
     */
    public function getParam(string $name)
    {
        return $this->_hit[$name] ?? [];
    }

    /**
     * Test if a param from the result hit is set.
     *
     * @param string $name Param name to test
     *
     * @return bool True if the param is set, false otherwise
     */
    public function hasParam(string $name): bool
    {
        return isset($this->_hit[$name]);
    }

    /**
     * Returns the hit id.
     *
     * @return string|int Hit id
     */
    public function getId()
    {
        return $this->getParam('_id');
    }

    /**
     * Returns the type of the result.
     *
     * @return string Result type
     */
    public function getType(): string
    {
        return $this->getParam('_type');
    }

    /**
     * Returns list of fields.
     *
     * @return array Fields list
     */
    public function getFields(): array
    {
        return $this->getParam('fields');
    }

    /**
     * Returns whether result has fields.
     *
     * @return bool
     */
    public function hasFields(): bool
    {
        return $this->hasParam('fields');
    }

    /**
     * Returns the index name of the result.
     *
     * @return string Index name
     */
    public function getIndex(): string
    {
        return $this->getParam('_index');
    }

    /**
     * Returns the score of the result.
     *
     * @return float Result score
     */
    public function getScore(): float
    {
        return $this->getParam('_score');
    }

    /**
     * Returns the raw hit array.
     *
     * @return array Hit array
     */
    public function getHit(): array
    {
        return $this->_hit;
    }

    /**
     * Returns the version information from the hit.
     *
     * @return string|int Document version
     */
    public function getVersion()
    {
        return $this->getParam('_version');
    }

    /**
     * Returns inner hits.
     *
     * @return array Fields list
     */
    public function getInnerHits(): array
    {
        return $this->getParam('inner_hits');
    }

    /**
     * Returns whether result has inner hits.
     *
     * @return bool
     */
    public function hasInnerHits(): bool
    {
        return $this->hasParam('inner_hits');
    }

    /**
     * Returns result data.
     *
     * Checks for partial result data with getFields, falls back to getSource or both
     *
     * @return array Result data array
     */
    public function getData(): array
    {
        if (isset($this->_hit['fields'])) {
            return isset($this->_hit['_source'])
                ? array_merge($this->getFields(), $this->getSource())
                : $this->getFields();
        }

        return $this->getSource();
    }

    /**
     * Returns the result source.
     *
     * @return array Source data array
     */
    public function getSource(): array
    {
        return $this->getParam('_source');
    }

    /**
     * Returns result data.
     *
     * @return array Result data array
     */
    public function getHighlights(): array
    {
        return $this->getParam('highlight');
    }

    /**
     * Returns explanation on how its score was computed.
     *
     * @return array explanations
     */
    public function getExplanation(): array
    {
        return $this->getParam('_explanation');
    }

    /**
     * Returns Document.
     *
     * @return Document
     */
    public function getDocument(): Document
    {
        $doc = new Document();
        $doc->setData($this->getSource());
        $hit = $this->getHit();
        unset($hit['_source']);
        unset($hit['_explanation']);
        unset($hit['highlight']);
        unset($hit['_score']);
        $doc->setParams($hit);

        return $doc;
    }

    /**
     * Sets a parameter on the hit.
     *
     * @param string $param
     * @param mixed  $value
     */
    public function setParam(string $param, $value)
    {
        $this->_hit[$param] = $value;
    }

    /**
     * Magic function to directly access keys inside the result.
     *
     * Returns null if key does not exist
     *
     * @param string $key Key name
     *
     * @return mixed Key value
     */
    public function __get(string $key)
    {
        $source = $this->getData();

        return $source[$key] ?? null;
    }

    /**
     * Magic function to support isset() calls.
     *
     * @param mixed $key Key name
     *
     * @return bool
     */
    public function __isset($key): bool
    {
        $source = $this->getData();

        return isset($source[$key]);
    }
}
