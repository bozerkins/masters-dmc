<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 04/03/2018
 * Time: 15:16
 */

namespace DataManagement\Model\EntityRelationship;


class TableHelper
{
    const COLUMN_TYPE_INTEGER   = 1;
    const COLUMN_TYPE_FLOAT     = 2;
    const COLUMN_TYPE_STRING    = 3;

    /**
     * @param array $column
     * @return string
     * @throws \Exception
     */
    public static function getFormatCode(array $column)
    {
        $type = $column['type'];
        if ($type === self::COLUMN_TYPE_INTEGER) {
            return 'i';
        }
        if ($type === self::COLUMN_TYPE_FLOAT) {
            return 'd';
        }
        if ($type === self::COLUMN_TYPE_STRING) {
            return 'Z' . (string) $column['size'];
        }
        throw new \Exception('undefined type for format code definition');
    }

    /**
     * @param int $type
     * @return int
     * @throws \Exception
     */
    public static function getSizeByType(int $type)
    {
        if ($type == self::COLUMN_TYPE_INTEGER) {
            return 4;
        }
        if ($type === self::COLUMN_TYPE_FLOAT) {
            return 8;
        }
        if ($type === self::COLUMN_TYPE_STRING) {
            return 255;
        }
        throw new \Exception('type unknown. could not define the default size');
    }

    /**
     * @param int $type
     * @throws \Exception
     */
    public static function validateType(int $type)
    {
        if ($type > 3 || $type < 1) {
            throw new \Exception('invalid type received');
        }
    }
}