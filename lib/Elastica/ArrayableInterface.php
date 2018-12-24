<?php

namespace Elastica;

/**
 * Interface for params.
 *
 *
 * @author Evgeniy Sokolov <ewgraf@gmail.com>
 */
interface ArrayableInterface
{
    /**
     * Converts the object to an array.
     *
     * @return array|object Object as array
     */
    public function toArray();
}
