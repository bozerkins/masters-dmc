<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 06/03/2018
 * Time: 23:36
 */

namespace DataManagement\Model\EntityRelationship;


class TableIndex
{
    private $table;
    private $bucketSize;

    public function __construct(Table $table, int $bucketSize)
    {
        $this->table = $table;
        $this->bucketSize = $bucketSize;
    }

    /**
     * @throws \Exception
     */
    public function initialize()
    {
        $this->table->create(
            [
                'pointer_left' => 0,
                'pointer_right' => 0,
                'pointer_next' => 0,
                'holder_value' => 0,
            ]
        );
    }

    /**
     * @param $value
     * @throws \Exception
     */
    public function create($value)
    {

    }

    public static function structure()
    {
        return array (
            1 =>
                array (
                    'id' => 1,
                    'name' => 'left',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
            2 =>
                array (
                    'id' => 2,
                    'name' => 'right',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
            3 =>
                array (
                    'id' => 3,
                    'name' => 'parent',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
            4 =>
                array (
                    'id' => 4,
                    'name' => 'value',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
        );
    }
}