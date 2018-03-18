<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 11/03/2018
 * Time: 22:02
 */

namespace DataManagement\Model\Index;


use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\EntityRelationship\TableHelper;

class Node
{
    /** @var Table */
    private $table;
    /** @var int */
    private $location;
    /** @var mixed */
    private $value;
    /** @var Node|null */
    private $parentNode;
    /** @var int */
    private $parentLocation;
    /** @var Node|null */
    private $nextInBucketNode;
    /** @var int */
    private $nextInBucketLocation;
    /** @var Node|null */
    private $leftNode;
    /** @var int */
    private $leftNodeLocation;
    /** @var Node|null */
    private $rightNode;
    /** @var int */
    private $rightNodeLocation;

    /**
     * Node constructor.
     * @param Table $table
     */
    public function __construct(Table $table)
    {
        $this->table = $table;
    }

    /**
     * @param int $location
     * @throws \Exception
     */
    public function loadFromStorage(int $location)
    {
        $iterator = $this->table->newIterator();
        $iterator->jump($location);
        $record = $iterator->read();
        $this->location = $location;
        $this->value = $record['value'];
        $this->parentLocation = $record['parent_node'] !== -1 ? $record['parent_node'] : null;
        $this->nextInBucketLocation = $record['next_in_bucket'] ?: null;
        $this->leftNodeLocation = $record['left_node'] ?: null;
        $this->rightNodeLocation = $record['right_node'] ?: null;
    }

    /**
     * @param $value
     * @throws \Exception
     */
    public function createOnStorage($value)
    {
        $iterator = $this->table->newIterator();
        $iterator->end();
        $iterator->create(
            [
                'parent_node' => -1,
                'next_in_bucket' => 0,
                'left_node' => 0,
                'right_node' => 0,
                'value' => $value
            ]
        );
        $this->value = $value;
        $iterator->rewind(1);
        $this->location = $iterator->position();
    }

    /**
     * @param int $valueType
     * @param int $valueSize
     * @return array
     * @throws \Exception
     */
    public static function structure(int $valueType, int $valueSize)
    {
        TableHelper::validateType($valueType);

        return array (
            array (
                'id' => 1,
                'name' => 'parent_node',
                'type' => TableHelper::COLUMN_TYPE_INTEGER,
                'size' => 4,
            ),
            array (
                'id' => 2,
                'name' => 'next_in_bucket',
                'type' => TableHelper::COLUMN_TYPE_INTEGER,
                'size' => 4,
            ),
            array (
                'id' => 3,
                'name' => 'left_node',
                'type' => TableHelper::COLUMN_TYPE_INTEGER,
                'size' => 4,
            ),
            array (
                'id' => 4,
                'name' => 'right_node',
                'type' => TableHelper::COLUMN_TYPE_INTEGER,
                'size' => 4,
            ),
            array (
                'id' => 5,
                'name' => 'value',
                'type' => $valueType,
                'size' => $valueSize,
            ),
        );
    }

    /**
     * @return mixed
     */
    public function value()
    {
        return $this->value;
    }

    /**
     * @return int
     */
    public function location()
    {
        return $this->location;
    }

    /**
     * @param Node $node
     * @throws \Exception
     */
    public function addParentNode(Node $node)
    {
        $this->parentNode = $node;
        $this->parentLocation = $node->location();
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['parent_node' => $node->location()]);
    }

    /**
     * @return bool
     */
    public function isRootBucket() : bool
    {
        return $this->parentLocation === null;
    }

    /**
     * @return Node
     * @throws \Exception
     */
    public function parentNode() : Node
    {
        if ($this->parentNode === null) {
            $this->parentNode = new Node($this->table);
            $this->parentNode->loadFromStorage($this->parentLocation);
        }
        return $this->parentNode;
    }

    /**
     * @param Node $node
     * @throws \Exception
     */
    public function addNextInBucket(Node $node)
    {
        $this->nextInBucketNode = $node;
        $this->nextInBucketLocation = $node->location();
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['next_in_bucket' => $node->location()]);
    }

    /**
     * @return bool
     */
    public function hasNextInBucket() : bool
    {
        return $this->nextInBucketLocation !== null;
    }

    /**
     * @return Node
     * @throws \Exception
     */
    public function nextInBucket() : Node
    {
        if ($this->nextInBucketNode === null) {
            $this->nextInBucketNode = new Node($this->table);
            $this->nextInBucketNode->loadFromStorage($this->nextInBucketLocation);
        }
        return $this->nextInBucketNode;
    }

    /**
     * @throws \Exception
     */
    public function detachNextInBucket()
    {
        $this->nextInBucketNode = null;
        $this->nextInBucketLocation = null;
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['next_in_bucket' => 0]);
    }

    /**
     * @param Node $node
     * @throws \Exception
     */
    public function addLeftNode(Node $node)
    {
        $this->leftNode = $node;
        $this->leftNodeLocation = $node->location();
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['left_node' => $node->location()]);
    }

    /**
     * @return bool
     */
    public function hasLeftNode() : bool
    {
        return $this->leftNodeLocation !== null;
    }

    /**
     * @return Node
     * @throws \Exception
     */
    public function leftNode() : Node
    {
        if ($this->leftNode === null) {
            $this->leftNode = new Node($this->table);
            $this->leftNode->loadFromStorage($this->leftNodeLocation);
        }
        return $this->leftNode;
    }

    /**
     * @throws \Exception
     */
    public function detachLeftNode()
    {
        $this->leftNode = null;
        $this->leftNodeLocation = null;
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['left_node' => 0]);
    }

    /**
     * @param Node $node
     * @throws \Exception
     */
    public function addRightNode(Node $node)
    {
        $this->rightNode = $node;
        $this->rightNodeLocation = $node->location();
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['right_node' => $node->location()]);
    }

    /**
     * @return bool
     */
    public function hasRightNode() : bool
    {
        return $this->rightNodeLocation !== null;
    }

    /**
     * @return Node
     * @throws \Exception
     */
    public function rightNode() : Node
    {
        if ($this->rightNode === null) {
            $this->rightNode = new Node($this->table);
            $this->rightNode->loadFromStorage($this->rightNodeLocation);
        }
        return $this->rightNode;
    }

    /**
     * @throws \Exception
     */
    public function detachRightNode()
    {
        $this->rightNode = null;
        $this->rightNodeLocation = null;
        $iterator = $this->table->newIterator();
        $iterator->jump($this->location);
        $iterator->update(['right_node' => 0]);
    }

    /**
     * @return bool
     */
    public function isLeaf() : bool
    {
        return $this->hasLeftNode() === false && $this->hasRightNode() === false;
    }
}