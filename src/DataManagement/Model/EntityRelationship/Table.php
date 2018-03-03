<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 2/28/2018
 * Time: 11:54 PM
 */

namespace DataManagement\Model\EntityRelationship;

use DataManagement\Storage\FileStorage;
use Symfony\Component\OptionsResolver\OptionsResolver;

class Table
{
    const COLUMN_TYPE_INTEGER   = 1;
    const COLUMN_TYPE_FLOAT     = 2;
    const COLUMN_TYPE_STRING    = 3;

    const OPERATION_READ_INCLUDE            = 1;
    const OPERATION_READ_STOP               = 2;
    const OPERATION_READ_INCLUDE_AND_STOP   = 3;

    const OPERATION_UPDATE_INCLUDE = 1;
    const OPERATION_UPDATE_STOP = 2;

    /** @var array with columns id, name, type, size */
    private $columns = [];
    /** @var FileStorage  */
    private $storage;

    /**
     * Table constructor.
     * @param FileStorage $storage
     */
    public function __construct(FileStorage $storage)
    {
        $this->storage = $storage;
    }

    /**
     * @return FileStorage
     */
    public function storage()
    {
        return $this->storage;
    }

    /**
     * @param array $record
     * @throws \Exception
     */
    public function create(array $record)
    {
        $this->storage->open('cb');

        $recordForPacking = [];
        $formatCodes = [];
        $recordSize = 0;

        foreach($this->columns as $column) {
            if (array_key_exists($column['name'], $record) !== true) {
                throw new \Exception(sprintf('missing column %s in the record', $column['name']));
            }
            $recordForPacking[] = $record[$column['name']];
            $formatCodes[] = $this->getFormatCode($column);
            $recordSize += $column['size'];
        }

        $recordPacked = pack(implode('', $formatCodes), ...$recordForPacking);

        $this->storage->acquire(LOCK_EX);

        fseek($this->storage->handle(), 0, SEEK_END);
        fwrite($this->storage->handle(), $recordPacked, $recordSize);

        $this->storage->close();
    }

    /**
     * @param \Closure $search
     * @return void
     * @throws \Exception
     */
    public function iterate(\Closure $search)
    {
        $this->storage->open('rb');
        $this->storage->acquire(LOCK_SH);
        $size = array_sum(array_column($this->columns, 'size'));
        $format = implode('/', array_map(function($column, $formatCode) {
            return $formatCode . $column['name'];
        }, $this->columns, array_map([$this, 'getFormatCode'], $this->columns)));

        while (feof($this->storage->handle()) === false) {
            $recordPacked = fread($this->storage->handle(), $size);
            if ($recordPacked === '') {
                continue;
            }
            $record = unpack($format, $recordPacked);
            $operation = $search($record);
            if ($operation === self::OPERATION_READ_STOP) {
                break;
            }
        }
        $this->storage->close();
    }

    /**
     * @param \Closure $search
     * @throws \Exception
     * @return array
     */
    public function read(\Closure $search) : array
    {
        $this->storage->open('rb');
        $this->storage->acquire(LOCK_SH);
        $size = array_sum(array_column($this->columns, 'size'));
        $format = implode('/', array_map(function($column, $formatCode) {
            return $formatCode . $column['name'];
        }, $this->columns, array_map([$this, 'getFormatCode'], $this->columns)));

        $result = [];
        while (feof($this->storage->handle()) === false) {
            $recordPacked = fread($this->storage->handle(), $size);
            if ($recordPacked === '') {
                continue;
            }
            $record = unpack($format, $recordPacked);
            $operation = $search($record);
            if ($operation === self::OPERATION_READ_INCLUDE) {
                $result[] = $record;
                continue;
            }
            if ($operation === self::OPERATION_READ_INCLUDE_AND_STOP) {
                $result[] = $record;
                break;
            }
            if ($operation === self::OPERATION_READ_STOP) {
                break;
            }
        }
        return $result;
    }

    /**
     * @param \Closure $search
     * @param \Closure $change
     * @throws \Exception
     */
    public function update(\Closure $search, \Closure $change)
    {
        $this->storage->open('c+b');
        $this->storage->acquire(LOCK_EX);
        $size = array_sum(array_column($this->columns, 'size'));
        $format = implode('/', array_map(function($column, $formatCode) {
            return $formatCode . $column['name'];
        }, $this->columns, array_map([$this, 'getFormatCode'], $this->columns)));
        while (feof($this->storage->handle()) === false) {
            $recordPacked = fread($this->storage->handle(), $size);
            if ($recordPacked === '') {
                continue;
            }
            $record = unpack($format, $recordPacked);
            $operation = $search($record);
            if ($operation === self::OPERATION_UPDATE_INCLUDE) {
                $updates = $change($record) ?? [];
                foreach($updates as $name => $update) {
                    $column = $this->getColumnByName($name);
                    $sizeUntilColumn = $this->getSizeUntilColumnByName($name);
                    fseek($this->storage->handle(), -$size+$sizeUntilColumn, SEEK_CUR );
                    $columnFormat = $this->getFormatCode($column);
                    $columnPacked = pack($columnFormat, $update);
                    fwrite($this->storage->handle(), $columnPacked, $column['size']);
                    fseek($this->storage->handle(), -$sizeUntilColumn-$column['size']+$size, SEEK_CUR );
                }
            }
            if ($operation === self::OPERATION_UPDATE_STOP) {
                break;
            }
        }
        $this->storage->close();
    }

    private function getSizeUntilColumnByName(string $name)
    {
        $size = 0;
        foreach($this->columns as $column) {
            if ($column['name'] === $name) {
                return $size;
            }
            $size += $column['size'];
        }
        throw new \Exception(sprintf('no column by the name %s found', $name));
    }

    private function getColumnByName(string $name)
    {
        foreach($this->columns as $column) {
            if ($column['name'] === $name) {
                return $column;
            }
        }
        throw new \Exception(sprintf('no column by the name %s found', $name));
    }

    /**
     * Add new column to the table structure. Returns the ID of the column added in the structure.
     *
     * @param string $name
     * @param int $type
     * @param int|null $size
     * @return int
     * @throws \Exception
     */
    public function addColumn(string $name, int $type, int $size = null) : int
    {
        $this->validateType($type);

        if ($size === null) {
            $size = $this->getSizeByType($type);
        }

        $column = [];
        $column['id'] = count($this->columns) + 1;
        $column['name'] = $name;
        $column['type'] = $type;
        $column['size'] = $size;
        $this->columns[] = $column;

        return $column['id'];
    }

    /**
     * @return array
     */
    public function structure()
    {
        return $this->columns;
    }

    /**
     * @param array $structure
     * @throws \Exception
     */
    public function load(array $structure)
    {
        $resolver = new OptionsResolver();
        $resolver->setRequired(array('id', 'name', 'type', 'size'));
        $resolver->setAllowedTypes('id', 'int');
        $resolver->setAllowedTypes('id', 'string');
        $resolver->setAllowedTypes('id', 'int');
        $resolver->setAllowedTypes('id', 'int');
        foreach($structure as $item) {
            $resolver->resolve($item);
        }

        usort($structure, function($a, $b) {
            return $a['id'] <=> $b['id'];
        });

        foreach($structure as $item) {
            $this->addColumn($item['name'], $item['type'], $item['size']);
        }
    }

    /**
     * @param array $column
     * @return string
     * @throws \Exception
     */
    private function getFormatCode(array $column)
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
    private function getSizeByType(int $type)
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
    private function validateType(int $type)
    {
        if ($type > 3 || $type < 1) {
            throw new \Exception('invalid type received');
        }
    }
}