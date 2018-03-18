<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 2/28/2018
 * Time: 11:54 PM
 */

namespace DataManagement\Model\Columnar;

use DataManagement\Model\TableHelper;
use DataManagement\Storage\FileStorage;
use DataManagement\Storage\FileStorageCompressed;
use DataManagement\Storage\FileStorageInterface;
use Symfony\Component\OptionsResolver\OptionsResolver;

class Table
{
    const OPERATION_READ_INCLUDE            = 1;
    const OPERATION_READ_STOP               = 2;
    const OPERATION_READ_INCLUDE_AND_STOP   = 3;

    /** @var array with columns id, name, type, size */
    private $columns = [];
    /** @var string  */
    private $dirname;

    /**
     * Table constructor.
     * @param string $dirname
     */
    public function __construct(string $dirname)
    {
        $this->dirname = $dirname;
    }

    /**
     * @return string
     */
    public function directory()
    {
        return $this->dirname;
    }

    /**
     * @throws \Exception
     */
    public function makeDirectory()
    {
        if (mkdir($this->dirname) === false) {
            throw new \Exception('failed to create the directory');
        }
    }

    /**
     * @param array $records
     * @throws \Exception
     */
    public function create(array $records)
    {
        $partitionColumn = $this->partitionColumn();
        $partitions = [];
        foreach($records as $record) {
            $partitionKey = $record[$partitionColumn['name']];
            if (array_key_exists($partitionKey, $partitions) === false) {
                $partitions[$partitionKey] = [];
            }
            $partitions[$partitionKey][] = $record;
        }

        foreach($partitions as $partitionKey => $partitionRecords) {
            /** @var TableIterator[] $iterators */
            $iterators = [];
            list($usec, $sec) = explode(" ", microtime());;
            $timestamp = $sec . $usec;
            foreach($this->structure() as $column) {
                $filename = 'column_' . $column['id'] . '_' . $timestamp;
                $storage = new FileStorageCompressed($this->dirname . '/' . $partitionKey . '/' . $filename, 9);
                $iterators[] = new TableIterator($column, $storage);
            }

            foreach($iterators as $iterator) {
                // prepare
                $columnsFromRecords = array_column($partitionRecords, $iterator->column()['name']);
                // take resources
                $iterator->storage()->create();
                $iterator->storage()->open('ab9');
                $iterator->storage()->acquire(LOCK_EX);
                // write
                $iterator->create($columnsFromRecords);
                // release
                $iterator->storage()->release();
                $iterator->storage()->close();
            }
        }
    }

    /**
     * @param string $partition
     * @throws \Exception
     */
    public function merge(string $partition)
    {
        /** @var TableIterator[] $iterators */
        $iterators = [];
        // open
        foreach($this->structure() as $column) {
            $filename = 'column_' . $column['id'] . '_' . $partition;
            $storage = new FileStorageCompressed($this->dirname . '/' . $partition . '/' . $filename, 9);
            $iterators[$column['id']] = new TableIterator($column, $storage);
            $iterators[$column['id']]->storage()->open('ab9');
            $iterators[$column['id']]->storage()->acquire(LOCK_EX);
        }
        // process
        if ($handle = opendir($this->dirname . '/' . $partition)) {

            $iteratorToRemove = [];

            while (false !== ($entry = readdir($handle))) {
                if ($entry === "." || $entry === "..") {
                    continue;
                }
                if (preg_match('/^column_[0-9]+_' . $partition . '.gz$/', $entry) === 1) {
                    continue;
                }
                if (preg_match('/.gz.lock$/', $entry) === 1) {
                    continue;
                }
                preg_match('/^column_([0-9]+)_/', $entry, $matches);
                $column = TableHelper::getColumnById($this->structure(), (int) $matches[1]);

                $storage = new FileStorageCompressed($this->dirname . '/' . $partition . '/' . preg_replace('/.gz$/', '', $entry), 9);
                /** @var TableIterator $iteratorFrom */
                $iteratorFrom = new TableIterator($column, $storage);
                $iteratorFrom->storage()->open('rb');
                $iteratorFrom->storage()->acquire(LOCK_EX);
                $iteratorFrom->jump(0);

                /** @var TableIterator $iteratorTo */
                $iteratorTo = $iterators[$column['id']];

                // transfer
                while ($iteratorFrom->endOfTable() === false) {
                    $binaryRecord = $iteratorFrom->storage()->read($iteratorFrom->rowCompleteSize());
                    if ($binaryRecord === '') {
                        continue;
                    }
                    $iteratorTo->storage()->write($binaryRecord, $iteratorTo->rowCompleteSize());
                }
                // schedule to remove
                $iteratorToRemove[] = $iteratorFrom;
            }
            foreach($iteratorToRemove as $iterator) {
                $iterator->storage()->remove();
            }
            closedir($handle);
        }
        // close
        foreach($iterators as $iterator) {
            $iterator->storage()->release();
            $iterator->storage()->close();
        }
    }

    public function partitions()
    {
        $partitions = [];
        if ($handle = opendir($this->dirname)) {
            while (false !== ($entry = readdir($handle))) {
                if ($entry === "." || $entry === "..") {
                    continue;
                }
                $partitions[] = $entry;
            }
            closedir($handle);
        }
        return $partitions;
    }

    /**
     * @param \Closure $search
     * @return void
     * @throws \Exception
     */
    public function iterate(array $columns, \Closure $search)
    {
        $columns = array_map(function($column) {
            return TableHelper::getColumnByName($this->structure(), $column);
        }, $columns);

        /** @var TableIterator[] $iterators */
        $iterators = [];
        foreach($this->partitions() as $partition) {
            foreach($columns as $column) {
                $filename = 'column_' . $column['id'] . '_' . $partition;
                $storage = new FileStorageCompressed($this->dirname . '/' . $partition . '/' . $filename, 9);
                $iterators[$column['name']] = new TableIterator($column, $storage);
                if ($iterators[$column['name']]->storage()->present() === false) {
                    $iterators[$column['name']]->storage()->create();
                }
                $iterators[$column['name']]->storage()->open('rb9');
                $iterators[$column['name']]->storage()->acquire(LOCK_SH);
            }

            while(true) {
                $isEndOfTable = false;
                foreach($iterators as $iterator) {
                    $isEndOfTable = $isEndOfTable || $iterator->storage()->eof();
                }
                if ($isEndOfTable) {
                    break;
                }
                $record = [];
                $isEmptyRecord = false;
                foreach($iterators as $columnName => $iterator) {
                    $read = $iterator->read();
                    if ($read !== null) {
                        $record[$columnName] = $read[$columnName];
                        continue;
                    }
                    $isEmptyRecord = true;
                }
                if ($isEmptyRecord === false) {
                    $operation = $search($record);
                    if ($operation === self::OPERATION_READ_STOP) {
                        break;
                    }
                }
            }
            foreach($iterators as $columnName => $iterator) {
                $iterator->storage()->release();
                $iterator->storage()->close();
            }
        }
    }

    /**
     * @param array $columns
     * @param \Closure $search
     * @return array
     * @throws \Exception
     */
    public function read(array $columns, \Closure $search) : array
    {

        $result = [];
        $this->iterate($columns, function($record) use (&$result, $search) {
            $operation = $search($record);
            if ($operation === self::OPERATION_READ_INCLUDE) {
                $result[] = $record;
                return null;
            }
            if ($operation === self::OPERATION_READ_INCLUDE_AND_STOP) {
                $result[] = $record;
                return self::OPERATION_READ_STOP;
            }
            if ($operation === self::OPERATION_READ_STOP) {
                return self::OPERATION_READ_STOP;
            }
        });
        return $result;
    }

    /**
     * Add new column to the table structure. Returns the ID of the column added in the structure.
     *
     * @param string $name
     * @param int $type
     * @param int|null $size
     * @param int $partition zero or one
     * @return int
     * @throws \Exception
     */
    public function addColumn(string $name, int $type, int $size = null, int $partition = 0) : int
    {
        TableHelper::validateType($type);

        if ($size === null) {
            $size = TableHelper::getSizeByType($type);
        }

        $column = [];
        $column['id'] = count($this->columns) + 1;
        $column['name'] = $name;
        $column['type'] = $type;
        $column['size'] = $size;
        $column['partition'] = $partition;
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
     * @return mixed
     * @throws \Exception
     */
    public function partitionColumn()
    {
        $partitionColumns = array_filter($this->structure(), function(array $column) {
            return $column['partition'];
        });
        if (count($partitionColumns) !== 1) {
            throw new \Exception('structure should contain exactly 1 partition column');
        }
        return array_pop($partitionColumns);
    }

    /**
     * @param array $structure
     * @throws \Exception
     */
    public function load(array $structure)
    {
        $resolver = new OptionsResolver();
        $resolver->setRequired(array('id', 'name', 'type', 'size', 'partition'));
        $resolver->setAllowedTypes('id', 'int');
        $resolver->setAllowedTypes('name', 'string');
        $resolver->setAllowedTypes('type', 'int');
        $resolver->setAllowedTypes('size', 'int');
        $resolver->setAllowedTypes('partition', 'int');
        foreach($structure as $item) {
            $resolver->resolve($item);
        }

        usort($structure, function($a, $b) {
            return $a['id'] <=> $b['id'];
        });

        foreach($structure as $item) {
            $this->addColumn($item['name'], $item['type'], $item['size'], $item['partition']);
        }

        if (array_sum(array_column($this->structure(), 'partition')) !== 1) {
            throw new \Exception('structure should contain exactly 1 partition column');
        }
    }
}