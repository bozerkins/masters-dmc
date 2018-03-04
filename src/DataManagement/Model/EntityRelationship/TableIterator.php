<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 04/03/2018
 * Time: 15:12
 */

namespace DataManagement\Model\EntityRelationship;


use DataManagement\Storage\FileStorage;

class TableIterator
{
    private $table;
    private $rowSize;
    private $rowFormat;
    private $rowWriteFormat;
    private $systemRowSize;
    private $systemRowFormat;
    private $systemRowWriteFormat;

    public function __construct(Table $table)
    {
        $this->table = $table;
        $this->rowSize = array_sum(array_column($table->structure(), 'size'));
        $this->rowFormat = implode('/', array_map(function($column, $formatCode) {
            return $formatCode . $column['name'];
        }, $table->structure(), array_map([TableHelper::class, 'getFormatCode'], $table->structure())));
        $this->systemRowSize = 1;
        $this->systemRowFormat = 'C1state';
    }

    public function skip(int $amountOfRecords)
    {
        fseek($this->table->storage()->handle(), $amountOfRecords * ($this->rowSize + $this->systemRowSize), SEEK_CUR);
    }

    public function rewind(int $amountOfRecords)
    {
        fseek($this->table->storage()->handle(), -1 * $amountOfRecords * ($this->rowSize + $this->systemRowSize), SEEK_CUR);
    }

    public function jump(int $positionOfRecord)
    {
        fseek($this->table->storage()->handle(), ($positionOfRecord - 1) * ($this->rowSize + $this->systemRowSize), SEEK_SET);
    }

    public function create(array $record)
    {

    }

    /**
     * @return array|null
     * @throws \Exception
     */
    public function read()
    {
        $systemRecordPacked = fread($this->table->storage()->handle(), $this->systemRowSize);
        if ($systemRecordPacked === '') {
            return null;
        }
        $systemRecord = unpack($this->systemRowFormat, $systemRecordPacked);
        if ($systemRecord['state'] === Table::INTERNAL_ROW_STATE_DELETE) {
            throw new \Exception('found deleted record');
        }
        $recordPacked = fread($this->table->storage()->handle(), $this->rowSize);
        return unpack($this->rowFormat, $recordPacked);
    }

    /**
     * @return bool
     */
    public function endOfTable()
    {
        return feof($this->table->storage()->handle());
    }
}