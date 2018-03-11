<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 11/03/2018
 * Time: 22:02
 */

namespace DataManagement\Model\Index;


class Node
{
    private $location;
    private $value;
    private $parentNode;
    private $nextInBucketNode;
    private $leftNode;
    private $rightNode;

    public function __construct($location, $value)
    {
        $this->location = $location;
        $this->value = $value;
    }

    public function value()
    {
        return $this->value;
    }

    public function location()
    {
        return $this->location;
    }

    public function addParentNode(Node $node)
    {
        $this->parentNode = $node;
    }

    public function isRootBucket() : bool
    {
        return $this->parentNode === null;
    }

    public function parentNode() : Node
    {
        return $this->parentNode;
    }

    public function addNextInBucket(Node $node)
    {
        $this->nextInBucketNode = $node;
    }

    public function hasNextInBucket() : bool
    {
        return $this->nextInBucketNode !== null;
    }

    public function nextInBucket() : Node
    {
        return $this->nextInBucketNode;
    }

    public function detachNextInBucket()
    {
        $this->nextInBucketNode = null;
    }

    public function addLeftNode(Node $node)
    {
        $this->leftNode = $node;
    }

    public function hasLeftNode() : bool
    {
        return $this->leftNode !== null;
    }

    public function leftNode() : Node
    {
        return $this->leftNode;
    }

    public function addRightNode(Node $node)
    {
        $this->rightNode = $node;
    }

    public function hasRightNode() : bool
    {
        return $this->rightNode !== null;
    }

    public function rightNode() : Node
    {
        return $this->rightNode;
    }

    public function isLeaf() : bool
    {
        return $this->hasLeftNode() === false && $this->hasRightNode() === false;
    }
}