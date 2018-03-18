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

    const OPERATION_READ_INCLUDE            = 1;
    const OPERATION_READ_STOP               = 2;
    const OPERATION_READ_INCLUDE_AND_STOP   = 3;

    const OPERATION_UPDATE_INCLUDE = 1;
    const OPERATION_UPDATE_STOP = 2;

    const OPERATION_DELETE_INCLUDE = 1;
    const OPERATION_DELETE_STOP = 2;
    const OPERATION_DELETE_INCLUDE_AND_STOP = 3;

    const RESERVE_READ = 1;
    const RESERVE_WRITE = 2;
    const RESERVE_READ_AND_WRITE = 3;

    /** @var array with columns id, name, type, size */
    private $columns = [];
    /** @var FileStorage  */
    private $storage;
    /** @var int|null  */
    private $reserve = null;

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
     * @return int
     */
    public function amountOfRecords()
    {
        clearstatcache($this->storage->file());
        $iterator = $this->newIterator();
        return filesize($this->storage->file()) / $iterator->rowCompleteSize();
    }

    /**
     * @return int
     * @throws \Exception
     */
    public function amountOfActiveRecords()
    {
        $counter = 0;
        $this->iterate(function($record) use (&$counter) {
            $counter++;
        });
        return $counter;
    }

    /**
     * @return TableIterator
     */
    public function newIterator()
    {
        return new TableIterator($this);
    }

    /**
     * @param array $record
     * @throws \Exception
     */
    public function create(array $record)
    {
        $canRelease = $this->tryReserve(self::RESERVE_WRITE);

        $iterator = $this->newIterator();
        $iterator->end();
        $iterator->create($record);

        $canRelease && $this->release();
    }

    /**
     * @param \Closure $search
     * @return void
     * @throws \Exception
     */
    public function iterate(\Closure $search)
    {
        $canRelease = $this->tryReserve(self::RESERVE_READ);

        $iterator = $this->newIterator();
        $iterator->jump(0);

        while ($iterator->endOfTable() === false) {
            $record = $iterator->read();
            if ($record === null) {
                continue;
            }
            $operation = $search($record, $iterator);
            if ($operation === self::OPERATION_READ_STOP) {
                break;
            }
        }

        $canRelease && $this->release();
    }

    /**
     * @param \Closure $search
     * @throws \Exception
     * @return array
     */
    public function read(\Closure $search) : array
    {
        $canRelease = $this->tryReserve(self::RESERVE_READ);

        $iterator = $this->newIterator();
        $iterator->jump(0);

        $result = [];

        while ($iterator->endOfTable() === false) {
            $record = $iterator->read();
            if ($record === null) {
                continue;
            }
            $operation = $search($record, $iterator);
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

        $canRelease && $this->release();

        return $result;
    }

    /**
     * @param \Closure $search
     * @param \Closure $change
     * @throws \Exception
     */
    public function update(\Closure $search, \Closure $change)
    {
        $canRelease = $this->tryReserve(self::RESERVE_READ_AND_WRITE);

        $iterator = $this->newIterator();
        $iterator->jump(0);

        while ($iterator->endOfTable() === false) {
            $record = $iterator->read();
            if ($record === null) {
                continue;
            }
            $operation = $search($record, $iterator);
            if ($operation === self::OPERATION_UPDATE_INCLUDE) {
                $updates = $change($record, $iterator) ?? [];
                $iterator->rewind(1);
                $iterator->update($updates);
                continue;
            }
            if ($operation === self::OPERATION_UPDATE_STOP) {
                break;
            }
        }

        $canRelease && $this->release();
    }

    /**
     * @param \Closure $search
     * @throws \Exception
     */
    public function delete(\Closure $search)
    {
        $canRelease = $this->tryReserve(self::RESERVE_READ_AND_WRITE);

        $iterator = $this->newIterator();
        $iterator->jump(0);

        while ($iterator->endOfTable() === false) {
            $record = $iterator->read();
            if ($record === null) {
                continue;
            }

            $operation = $search($record, $iterator);
            if ($operation === self::OPERATION_DELETE_INCLUDE) {
                $iterator->rewind(1);
                $iterator->delete();
                continue;
            }
            if ($operation === self::OPERATION_DELETE_INCLUDE_AND_STOP) {
                $iterator->rewind(1);
                $iterator->delete();
                break;
            }
            if ($operation === self::OPERATION_DELETE_STOP) {
                break;
            }
        }

        $canRelease && $this->release();
    }

    /**
     * @param int $mode
     * @throws \Exception
     */
    public function reserve(int $mode)
    {
        if ($this->reserve !== null) {
            throw new \Exception('cannot reserve the resource, because it\'s already reserved');
        }
        $this->reserve = $mode;
        if ($this->reserve === self::RESERVE_READ) {
            $this->storage->open('rb');
            $this->storage->acquire(LOCK_SH);
            return;
        }
        if ($this->reserve === self::RESERVE_WRITE) {
            $this->storage->open('cb');
            $this->storage->acquire(LOCK_EX);
            return;
        }
        if ($this->reserve === self::RESERVE_READ_AND_WRITE) {
            $this->storage->open('c+b');
            $this->storage->acquire(LOCK_EX);
            return;
        }
        throw new \Exception('invalid reserve mode passed');
    }

    /**
     * @param int $mode
     * @return bool if the operation was run
     * @throws \Exception
     */
    private function tryReserve(int $mode) : bool
    {
        if ($this->reserve === null) {
            $this->reserve($mode);
            return true;
        }
        if ($mode === $this->reserve) {
            return false;
        }
        if (
            ($mode === self::RESERVE_READ || $mode === self::RESERVE_WRITE)
            && ($this->reserve === self::RESERVE_READ_AND_WRITE)
        ) {
            return false;
        }
        throw new \Exception('currently active reserve mode is not compatible with required operation');
    }

    /**
     * @throws \Exception
     */
    public function release()
    {
        if ($this->reserve === null) {
            throw new \Exception('reserve is not set, nothing to release');
        }
        $this->storage()->release();
        $this->storage->close();
        $this->reserve = null;
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
        TableHelper::validateType($type);

        if ($size === null) {
            $size = TableHelper::getSizeByType($type);
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
}