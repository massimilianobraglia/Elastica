<?php

namespace Elastica\Suggest;

use Elastica\Exception\InvalidException;
use Elastica\NameableInterface;
use Elastica\Param;

/**
 * Class AbstractSuggestion.
 */
abstract class AbstractSuggest extends Param implements NameableInterface
{
    /**
     * @var string the name of this suggestion
     */
    protected $_name;

    /**
     * @param string $name
     * @param string $field
     */
    public function __construct($name, $field)
    {
        $this->setName($name);
        $this->setField($field);
    }

    /**
     * Suggest text must be set either globally or per suggestion.
     *
     * @param string $text
     *
     * @return $this
     */
    public function setText($text)
    {
        return $this->_setRawParam('text', $text);
    }

    /**
     * Suggest prefix must be set either globally or per suggestion.
     *
     * @param string $text
     *
     * @return $this
     */
    public function setPrefix($prefix)
    {
        return $this->_setRawParam('prefix', $prefix);
    }

    /**
     * Suggest regex must be set either globally or per suggestion.
     *
     * @param string $text
     *
     * @return $this
     */
    public function setRegex($regex)
    {
        return $this->_setRawParam('regex', $regex);
    }

    /**
     * Expects one of the next params: max_determinized_states - defaults to 10000,
     * flags are ALL (default), ANYSTRING, COMPLEMENT, EMPTY, INTERSECTION, INTERVAL, or NONE.
     *
     * @param array $value
     *
     * @return $this
     */
    public function setRegexOptions(array $value)
    {
        return $this->setParam('regex', $value);
    }

    /**
     * @param string $field
     *
     * @return $this
     */
    public function setField($field)
    {
        return $this->setParam('field', $field);
    }

    /**
     * @param int $size
     *
     * @return $this
     */
    public function setSize($size)
    {
        return $this->setParam('size', $size);
    }

    /**
     * @param int $size maximum number of suggestions to be retrieved from each shard
     *
     * @return $this
     */
    public function setShardSize($size)
    {
        return $this->setParam('shard_size', $size);
    }

    /**
     * {@inheritdoc}
     *
     * @return $this
     */
    public function setName(string $name): NameableInterface
    {
        if (empty($name)) {
            throw new InvalidException('Suggest name has to be set');
        }

        $this->_name = $name;

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function getName(): string
    {
        return $this->_name;
    }
}
