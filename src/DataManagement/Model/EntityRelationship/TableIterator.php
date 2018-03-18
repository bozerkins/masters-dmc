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
    const INTERNAL_ROW_STATE_ACTIVE = 97; // a
    const INTERNAL_ROW_STATE_DELETE = 100; // d

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
        $this->rowWriteFormat = implode('', array_map([TableHelper::class, 'getFormatCode'], $table->structure()));
        $this->systemRowSize = 1;
        $this->systemRowFormat = 'C1state';
        $this->systemRowWriteFormat = 'C';
    }

    public function rowCompleteSize()
    {
        return $this->rowSize + $this->systemRowSize;
    }

    public function table()
    {
        return $this->table;
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
        fseek($this->table->storage()->handle(), $positionOfRecord * ($this->rowSize + $this->systemRowSize), SEEK_SET);
    }

    public function end()
    {
        fseek($this->table->storage()->handle(), 0, SEEK_END);
    }

    public function position()
    {
        return (int) floor(ftell($this->table->storage()->handle()) / ($this->systemRowSize + $this->rowSize));
    }

    /**
     * @param int $status
     * @return string
     */
    private function packSystemRecord(int $status) : string
    {
        return pack($this->systemRowWriteFormat, $status);
    }

    /**
     * @param array $record
     * @return string
     * @throws \Exception
     */
    private function packRecord(array $record) : string
    {
        $recordForPacking = [];

        foreach($this->table->structure() as $column) {
            if (array_key_exists($column['name'], $record) !== true) {
                throw new \Exception(sprintf('missing column %s in the record', $column['name']));
            }
            $recordForPacking[] = $record[$column['name']];
        }
        return pack($this->rowWriteFormat, ...$recordForPacking);
    }

    /**
     * @param string $binaryRecord
     * @return array
     */
    private function unpackSystemRecord(string $binaryRecord)
    {
        $state = substr($binaryRecord, 0, $this->systemRowSize);
        return unpack($this->systemRowFormat, $state);
    }

    /**
     * @param string $binaryRecord
     * @return array
     */
    private function unpackRecord(string $binaryRecord)
    {
        $record = substr($binaryRecord, $this->systemRowSize, $this->rowSize);
        return unpack($this->rowFormat, $record);
    }

    /**
     * @param array $record
     * @throws \Exception
     */
    public function create(array $record)
    {
        $binaryRecord = '';
        $binaryRecord .= $this->packSystemRecord(self::INTERNAL_ROW_STATE_ACTIVE);
        $binaryRecord .= $this->packRecord($record);
        fwrite($this->table->storage()->handle(), $binaryRecord, $this->systemRowSize + $this->rowSize);
    }

    /**
     * @return array|null
     * @throws \Exception
     */
    public function read()
    {
        $binaryRecord = fread($this->table->storage()->handle(), $this->systemRowSize + $this->rowSize);
        if ($binaryRecord === '') {
            return null;
        }
        $systemRecord = $this->unpackSystemRecord($binaryRecord);
        if ($systemRecord['state'] === self::INTERNAL_ROW_STATE_DELETE) {
            return null;
        }
        return $this->unpackRecord($binaryRecord);
    }

    /**
     * @param array $updates
     * @throws \Exception
     */
    public function update(array $updates)
    {
        // initialize jump size
        $sizeToJump = 0;
        // skip system row
        fseek($this->table->storage()->handle(), +$this->systemRowSize, SEEK_CUR );
        // go through the structure
        foreach($this->table->structure() as $column) {
            if (array_key_exists($column['name'], $updates) === false) {
                $sizeToJump += $column['size'];
                continue;
            }
            $columnPacked = pack(TableHelper::getFormatCode($column), $updates[$column['name']]);
            // jump to column beginning
            fseek($this->table->storage()->handle(), $sizeToJump, SEEK_CUR );
            // update the column
            fwrite($this->table->storage()->handle(), $columnPacked, $column['size']);
            // reset size to jump counter
            $sizeToJump = 0;
        }
        // if anything left to jump
        if ($sizeToJump > 0) {
            // jump to the end of the row
            fseek($this->table->storage()->handle(), $sizeToJump, SEEK_CUR );
        }
    }

    public function delete()
    {
        $binarySystemRecord = $this->packSystemRecord(self::INTERNAL_ROW_STATE_DELETE);
        // write delete status
        fwrite($this->table->storage()->handle(), $binarySystemRecord, $this->systemRowSize);
        // return to the beginning of the row
        fseek($this->table->storage()->handle(), +$this->rowSize, SEEK_CUR );
    }

    /**
     * @return bool
     */
    public function endOfTable()
    {
        return feof($this->table->storage()->handle());
    }
}