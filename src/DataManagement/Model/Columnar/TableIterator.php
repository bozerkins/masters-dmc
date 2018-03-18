<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 04/03/2018
 * Time: 15:12
 */

namespace DataManagement\Model\Columnar;


use DataManagement\Model\TableHelper;
use DataManagement\Storage\FileStorageCompressed;

class TableIterator
{
    private $storage;
    private $column;
    private $rowSize;
    private $rowFormat;
    private $rowWriteFormat;

    /**
     * TableIterator constructor.
     * @param array $column
     * @param FileStorageCompressed $storage
     * @throws \Exception
     */
    public function __construct(array $column, FileStorageCompressed $storage)
    {
        $this->storage = $storage;
        $this->column = $column;
        $this->rowSize = $column['size'];
        $this->rowFormat = TableHelper::getFormatCode($column) . $column['name'];
        $this->rowWriteFormat = TableHelper::getFormatCode($column);
    }

    public function storage()
    {
        return $this->storage;
    }

    public function column()
    {
        return $this->column;
    }

    public function rowCompleteSize()
    {
        return $this->rowSize;
    }

    public function skip(int $amountOfRecords)
    {
        $this->storage->seek($amountOfRecords * ($this->rowSize), SEEK_CUR);
    }

    public function rewind(int $amountOfRecords)
    {
        $this->storage->seek(-1 * $amountOfRecords * ($this->rowSize), SEEK_CUR);
    }

    public function jump(int $positionOfRecord)
    {
        $this->storage->seek($positionOfRecord * ($this->rowSize), SEEK_SET);
    }

    public function end()
    {
        $this->storage->seek(0, SEEK_END);
    }

    public function position()
    {
        return (int) floor($this->storage->tell() / ($this->rowSize));
    }

    /**
     * @param array $columns
     * @return string
     * @throws \Exception
     */
    private function packColumns(array $columns) : string
    {
        $binaryRecord = '';
        foreach($columns as $column) {
            $binaryRecord .= pack($this->rowWriteFormat, $column);
        }
        return $binaryRecord;
    }

    /**
     * @param string $binaryRecord
     * @return array
     */
    private function unpackRecord(string $binaryRecord)
    {
        return unpack($this->rowFormat, $binaryRecord);
    }

    /**
     * @param array $columns
     * @throws \Exception
     */
    public function create(array $columns)
    {
        $this->storage->write($this->packColumns($columns), $this->rowSize * count($columns));
    }

    /**
     * @return array|null
     * @throws \Exception
     */
    public function read()
    {
        $binaryRecord = $this->storage->read($this->rowSize);
        if ($binaryRecord === '') {
            return null;
        }
        return $this->unpackRecord($binaryRecord);
    }

    /**
     * @return bool
     */
    public function endOfTable()
    {
        return $this->storage->eof();
    }
}