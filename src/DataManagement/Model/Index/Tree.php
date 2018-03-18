<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 11/03/2018
 * Time: 22:02
 */

namespace DataManagement\Model\Index;


class Tree
{
    /**
     * @var Node
     */
    private $root = null;
    /**
     * @var int
     */
    private $bucketSize = null;

    public $debug = false;

    public function __construct(int $bucketSize = 4)
    {
        $this->root = new Node(null, null);
        $this->bucketSize = $bucketSize;
    }

    /**
     * @param Node $node
     * @throws \Exception
     */
    public function create(Node $node)
    {
        // handle first node in the tree
        if ($this->root->hasLeftNode() === false) {
            $this->root->addLeftNode($node);
            $node->addParentNode($this->root);
            return;
        }

        $firstBucketNode = $this->search($node->value());
        $this->addToBucket($firstBucketNode, $node);
        $this->splitBucketIfFull($this->bucketFirstNode($node));
    }

    /**
     * @param $value
     * @return Node
     * @throws \Exception
     */
    public function read($value) : Node
    {
        $node = $this->search($value);
        while(true) {
            if ($node->value() === $value) {
                return $node;
            }
            if ($node->hasNextInBucket() === false) {
                break;
            }
            $node = $node->nextInBucket();
        }
        throw new \Exception('value not found');
    }

    /**
     * @param Node $node
     * @return Node
     * @throws \Exception
     */
    private function bucketFirstNode(Node $node) : Node
    {
        $parentNode = $node->parentNode();
        if ($parentNode->isRootBucket()) {
            return $parentNode->leftNode();
        }
        if ($parentNode->hasRightNode() && $parentNode->value() < $node->value()) {
            return $parentNode->rightNode();
        }
        if ($parentNode->hasLeftNode() && $parentNode->value() > $node->value()) {
            return $parentNode->leftNode();
        }
        throw new \Exception('could not find bucket first node');
    }

    /**
     * @param Node $firstBucketNode
     * @param Node $node
     * @throws \Exception
     */
    private function addToBucket(Node $firstBucketNode, Node $node)
    {
        // link to parent
        $node->addParentNode($firstBucketNode->parentNode());

        // add as first node of bucket
        if ($firstBucketNode->value() > $node->value()) {
            // insert as the most left
            $node->addNextInBucket($firstBucketNode);
            // share link left and right nodes
            if ($node->hasRightNode()) {
                $firstBucketNode->addLeftNode($node->rightNode());
            }

            // link to parent
            $parentNode = $firstBucketNode->parentNode();
            if ($parentNode->leftNode()->location() === $firstBucketNode->location()) {
                $parentNode->addLeftNode($node);
                return;
            }
            if ($parentNode->rightNode()->location() === $firstBucketNode->location()) {
                $parentNode->addRightNode($node);
                if ($parentNode->hasNextInBucket()) {
                    $parentNode->nextInBucket()->addLeftNode($node);
                }
                return;
            }

            throw new \Exception('undefined parent node found');
        }

        $leftBucketNode = $firstBucketNode;
        while($leftBucketNode->hasNextInBucket()) {
            if ($leftBucketNode->value() < $node->value() && $leftBucketNode->nextInBucket()->value() > $node->value()) {
                // insert between
                $rightBucketNode = $leftBucketNode->nextInBucket();
                $leftBucketNode->addNextInBucket($node);
                if ($node->hasLeftNode()) {
                    $leftBucketNode->addRightNode($node->leftNode());
                }
                $node->addNextInBucket($rightBucketNode);
                if ($node->hasRightNode()) {
                    $rightBucketNode->addLeftNode($node->rightNode());
                }
                return;
            }
            $leftBucketNode = $leftBucketNode->nextInBucket();
        }

        // insert as the most right
        $leftBucketNode->addNextInBucket($node);
        if ($node->hasLeftNode()) {
            $leftBucketNode->addRightNode($node->leftNode());
        }
    }

    /**
     * Returns the first node of the bucket
     * @param mixed $value
     * @return Node
     * @throws \Exception
     */
    private function search($value) : Node
    {
        /** @var Node $searchNode */
        $searchNode = $this->root->leftNode();

        do {
            // is middle of the tree
            if ($searchNode->isLeaf() === false) {
                // if the search node is more, then jump left right away
                if ($searchNode->value() > $value) {
                    $searchNode = $searchNode->leftNode();
                    continue;
                }
                // has next bucket
                if ($searchNode->hasNextInBucket()) {
                    // check if node value fits
                    if ($searchNode->value() < $value && $searchNode->nextInBucket()->value() > $value) {
                        // go down the tree
                        $searchNode = $searchNode->rightNode();
                        continue;
                    }
                    // go further right in bucket
                    $searchNode = $searchNode->nextInBucket();
                    continue;
                }
                // has no next bucket - check if we can jump right
                if ($searchNode->value() < $value) {
                    $searchNode = $searchNode->rightNode();
                    continue;
                }
                // value already exists
                throw new \Exception('value already exists');
            }
            // is end leaf
            // then it's the beginning of the bucket anyways!
            return $searchNode;

        } while (true);
    }

    /**
     * @param Node $node
     * @throws \Exception
     */
    private function splitBucketIfFull(Node $node)
    {
        if ($this->isBucketFull($node) === false) {
            return;
        }
        $parentNode = $this->splitBucket($node);
        if ($parentNode->isRootBucket() === false) {
            $this->splitBucketIfFull($parentNode);
        }
    }

    /**
     * @param Node $node
     * @return Node
     * @throws \Exception
     */
    private function splitBucket(Node $node) : Node
    {
        $firstBucketSize = (int) floor($this->bucketSize / 2);
        $secondBucketSize = (int) ceil($this->bucketSize / 2);
        /** @var Node[] $firstBucket */
        $firstBucket = [];
        /** @var Node[] $secondBucket */
        $secondBucket = [];
        $iterationNode = $node;
        $iterationCounter = 0;
        while($iterationNode->hasNextInBucket()) {
            $iterationCounter++;
            if ($iterationCounter <= $firstBucketSize) {
                $firstBucket[] = $iterationNode;
                $iterationNode = $iterationNode->nextInBucket();
                continue;
            }
            if ($iterationCounter <= ($firstBucketSize + $secondBucketSize)) {
                $secondBucket[] = $iterationNode;
                $iterationNode = $iterationNode->nextInBucket();
                continue;
            }
            throw new \Exception('some miscalculation happened while splitting bucket');
        }
        // add last node into second bucket
        $secondBucket[] = $iterationNode;
        // save bucket root node
        $bucketRootNode = array_shift($secondBucket);

        // bind new buckets to the root node
        $bucketRootNode->addLeftNode($firstBucket[0]);
        $bucketRootNode->addRightNode($secondBucket[0]);
        // detach next nodes in the new buckets
        $firstBucket[count($firstBucket) - 1]->detachNextInBucket();
        $secondBucket[count($secondBucket) - 1]->detachNextInBucket();
        // detach next node in the new root node
        $bucketRootNode->detachNextInBucket();
        // reattach parents
        $parentNode = $bucketRootNode->parentNode();
        if ($parentNode->isRootBucket()) {
            $parentNode->addLeftNode($bucketRootNode);
        } else {
            $this->addToBucket($this->bucketFirstNode($parentNode), $bucketRootNode);
        }
        // attach new buckets to the new parent
        foreach(array_merge($firstBucket, $secondBucket) as $bucketNode) {
            /** @var Node $bucketNode */
            $bucketNode->addParentNode($bucketRootNode);
        }

        // return new parent node
        return $bucketRootNode;
    }

    private function isBucketFull(Node $node) : bool
    {
        $counter = 1;
        while($node->hasNextInBucket()) {
            $counter++;
            $node = $node->nextInBucket();
        }
        return $counter >= $this->bucketSize;
    }

    public function display() : string
    {
        $response = '';
        $start = $this->root->leftNode();
        $depth = 0;
        $response .= $this->displayRecursively($start, $depth + 1);
        return $response;

    }

    private function displayRecursively(Node $node, int $depth) : string
    {
        $response = '';
        $prefix = str_repeat("\t", $depth);
        do {
            $response .= $prefix . $node->value() . '(' . $node->location() . ')' . PHP_EOL;
            if ($node->hasLeftNode()) {
                $response .= $prefix . 'left' . PHP_EOL;
                $response .= $this->displayRecursively($node->leftNode(), $depth + 1);
            }
            if ($node->hasRightNode()) {
                $response .= $prefix . 'right' . PHP_EOL;
                $response .= $this->displayRecursively($node->rightNode(), $depth + 1);
            }
            if ($node->hasNextInBucket() === false) {
                break;
            }
            $node = $node->nextInBucket();
        } while(true);

        return $response;
    }
}