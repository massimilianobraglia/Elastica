<?php

namespace Elastica;

use Elastica\Exception\InvalidException;

/**
 * Base class for things that can be sent to the update api (Document and
 * Script).
 *
 * @author   Nik Everett <nik9000@gmail.com>
 */
class AbstractUpdateAction extends Param
{
    /**
     * @var \Elastica\Document
     */
    protected $_upsert;

    /**
     * Sets the id of the document.
     *
     * @param string|int $id
     *
     * @return $this
     */
    public function setId($id): self
    {
        return $this->setParam('_id', $id);
    }

    /**
     * Returns document id.
     *
     * @return string|int Document id
     */
    public function getId()
    {
        return $this->hasParam('_id') ? $this->getParam('_id') : null;
    }

    /**
     * @return bool
     */
    public function hasId(): bool
    {
        return '' !== (string) $this->getId();
    }

    /**
     * Sets the document type name.
     *
     * @param Type|string $type Type name
     *
     * @return $this
     */
    public function setType($type): self
    {
        if ($type instanceof Type) {
            $this->setIndex($type->getIndex());
            $type = $type->getName();
        }

        return $this->setParam('_type', $type);
    }

    /**
     * Return document type name.
     *
     * @throws InvalidException
     *
     * @return string Document type name
     */
    public function getType(): string
    {
        return $this->getParam('_type');
    }

    /**
     * Sets the document index name.
     *
     * @param Index|string $index Index name
     *
     * @return $this
     */
    public function setIndex($index): self
    {
        if ($index instanceof Index) {
            $index = $index->getName();
        }

        return $this->setParam('_index', $index);
    }

    /**
     * Get the document index name.
     *
     * @throws InvalidException
     *
     * @return string Index name
     */
    public function getIndex(): string
    {
        return $this->getParam('_index');
    }

    /**
     * Sets the version of a document for use with optimistic concurrency control.
     *
     * @param int $version Document version
     *
     * @return $this
     *
     * @see https://www.elastic.co/blog/versioning
     */
    public function setVersion(int $version): self
    {
        return $this->setParam('_version', $version);
    }

    /**
     * Returns document version.
     *
     * @return int Document version
     */
    public function getVersion(): int
    {
        return $this->getParam('_version');
    }

    /**
     * @return bool
     */
    public function hasVersion(): bool
    {
        return $this->hasParam('_version');
    }

    /**
     * Sets the version_type of a document
     * Default in ES is internal, but you can set to external to use custom versioning.
     *
     * @param string $versionType Document version type
     *
     * @return $this
     */
    public function setVersionType(string $versionType = 'internal')
    {
        return $this->setParam('_version_type', $versionType);
    }

    /**
     * Returns document version type.
     *
     * @return string Document version type
     */
    public function getVersionType(): string
    {
        return $this->getParam('_version_type');
    }

    /**
     * @return bool
     */
    public function hasVersionType(): bool
    {
        return $this->hasParam('_version_type');
    }

    /**
     * Sets parent document id.
     *
     * @param string|int $parent Parent document id
     *
     * @return $this
     *
     * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-parent-field.html
     */
    public function setParent($parent): self
    {
        return $this->setParam('_parent', $parent);
    }

    /**
     * Returns the parent document id.
     *
     * @return string|int Parent document id
     */
    public function getParent()
    {
        return $this->getParam('_parent');
    }

    /**
     * @return bool
     */
    public function hasParent(): bool
    {
        return $this->hasParam('_parent');
    }

    /**
     * Set operation type.
     *
     * @param string $opType Only accept create
     *
     * @return $this
     */
    public function setOpType(string $opType): self
    {
        return $this->setParam('_op_type', $opType);
    }

    /**
     * Get operation type.
     *
     * @return string
     */
    public function getOpType(): string
    {
        return $this->getParam('_op_type');
    }

    /**
     * @return bool
     */
    public function hasOpType(): bool
    {
        return $this->hasParam('_op_type');
    }

    /**
     * Set routing query param.
     *
     * @param string $value routing
     *
     * @return $this
     */
    public function setRouting(string $value): self
    {
        return $this->setParam('_routing', $value);
    }

    /**
     * Get routing parameter.
     *
     * @return string
     */
    public function getRouting(): string
    {
        return $this->getParam('_routing');
    }

    /**
     * @return bool
     */
    public function hasRouting(): bool
    {
        return $this->hasParam('_routing');
    }

    /**
     * @param array|string $fields
     *
     * @return $this
     */
    public function setFields($fields): self
    {
        if (is_array($fields)) {
            $fields = implode(',', $fields);
        }

        return $this->setParam('_fields', (string) $fields);
    }

    /**
     * @return $this
     */
    public function setFieldsSource(): self
    {
        return $this->setFields('_source');
    }

    /**
     * @return string
     */
    public function getFields(): string
    {
        return $this->getParam('_fields');
    }

    /**
     * @return bool
     */
    public function hasFields(): bool
    {
        return $this->hasParam('_fields');
    }

    /**
     * @param int $num
     *
     * @return $this
     */
    public function setRetryOnConflict(int $num): self
    {
        return $this->setParam('_retry_on_conflict', $num);
    }

    /**
     * @return int
     */
    public function getRetryOnConflict(): int
    {
        return $this->getParam('_retry_on_conflict');
    }

    /**
     * @return bool
     */
    public function hasRetryOnConflict(): bool
    {
        return $this->hasParam('_retry_on_conflict');
    }

    /**
     * @param bool $refresh
     *
     * @return $this
     */
    public function setRefresh(bool $refresh = true): self
    {
        return $this->setParam('_refresh', $refresh);
    }

    /**
     * @return bool
     */
    public function getRefresh(): bool
    {
        return $this->getParam('_refresh');
    }

    /**
     * @return bool
     */
    public function hasRefresh(): bool
    {
        return $this->hasParam('_refresh');
    }

    /**
     * @param string $timeout
     *
     * @return $this
     */
    public function setTimeout(string $timeout): self
    {
        return $this->setParam('_timeout', $timeout);
    }

    /**
     * @return string
     */
    public function getTimeout(): string
    {
        return $this->getParam('_timeout');
    }

    /**
     * @return bool
     */
    public function hasTimeout(): bool
    {
        return $this->hasParam('_timeout');
    }

    /**
     * @param string $consistency
     *
     * @return $this
     */
    public function setConsistency(string $consistency): self
    {
        return $this->setParam('_consistency', $consistency);
    }

    /**
     * @return string
     */
    public function getConsistency(): string
    {
        return $this->getParam('_consistency');
    }

    /**
     * @return bool
     */
    public function hasConsistency(): bool
    {
        return $this->hasParam('_consistency');
    }

    /**
     * @param string $replication
     *
     * @return $this
     */
    public function setReplication(string $replication): self
    {
        return $this->setParam('_replication', $replication);
    }

    /**
     * @return string
     */
    public function getReplication(): string
    {
        return $this->getParam('_replication');
    }

    /**
     * @return bool
     */
    public function hasReplication(): bool
    {
        return $this->hasParam('_replication');
    }

    /**
     * @param Document|array $data
     *
     * @return $this
     */
    public function setUpsert($data): self
    {
        $document = Document::create($data);
        $this->_upsert = $document;

        return $this;
    }

    /**
     * @return Document
     */
    public function getUpsert(): Document
    {
        return $this->_upsert;
    }

    /**
     * @return bool
     */
    public function hasUpsert(): bool
    {
        return null !== $this->_upsert;
    }

    /**
     * @param array $fields         if empty array all options will be returned, field names can be either with underscored either without, i.e. _percolate, routing
     * @param bool  $withUnderscore should option keys contain underscore prefix
     *
     * @return array
     */
    public function getOptions(array $fields = [], bool $withUnderscore = false): array
    {
        if (!empty($fields)) {
            $data = [];
            foreach ($fields as $field) {
                $key = '_'.ltrim($field, '_');
                if ($this->hasParam($key) && '' !== (string) $this->getParam($key)) {
                    $data[$key] = $this->getParam($key);
                }
            }
        } else {
            $data = $this->getParams();
        }
        if (!$withUnderscore) {
            foreach ($data as $key => $value) {
                $data[ltrim($key, '_')] = $value;
                unset($data[$key]);
            }
        }

        return $data;
    }
}
