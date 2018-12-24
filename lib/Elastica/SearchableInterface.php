<?php

namespace Elastica;

/**
 * Elastica searchable interface.
 *
 * @author Thibault Duplessis <thibault.duplessis@gmail.com>
 */
interface SearchableInterface
{
    /**
     * Searches results for a query.
     *
     * {
     *     "from" : 0,
     *     "size" : 10,
     *     "sort" : {
     *          "postDate" : {"order" : "desc"},
     *          "user" : { },
     *          "_score" : { }
     *      },
     *      "query" : {
     *          "term" : { "user" : "kimchy" }
     *      }
     * }
     *
     * @param string|array|Query $query   Array with all query data inside or a Elastica\Query object
     * @param int|array|null     $options
     *
     * @return ResultSet with all results inside
     */
    public function search($query = '', $options = null): ResultSet;

    /**
     * Counts results for a query.
     *
     * If no query is set, matchall query is created
     *
     * @param string|array|Query $query Array with all query data inside or a Elastica\Query object
     *
     * @return int number of documents matching the query
     */
    public function count($query = ''): int;

    /**
     * @param Query|string   $query
     * @param array|int|null $options
     *
     * @return Search
     */
    public function createSearch($query = '', $options = null): Search;
}
