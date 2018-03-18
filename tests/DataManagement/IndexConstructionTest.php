<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 03/03/2018
 * Time: 12:28
 */

namespace DataManagement;

use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\EntityRelationship\TableHelper;
use DataManagement\Model\EntityRelationship\TableIndex;
use DataManagement\Model\EntityRelationship\TableIterator;
use DataManagement\Model\Index\Node;
use DataManagement\Model\Index\Tree;
use DataManagement\Storage\FileStorage;
use PHPUnit\Framework\TestCase;

class IndexConstructionTest extends TestCase
{
    /**
     * @throws \Exception
     */
    public function testIndexFormationSimple()
    {
        $tree = new Tree();
        foreach([5,7,3,1,2] as $index => $value) {
            $node = new Node($index, $value);
            $tree->create($node);
        }

        $this->assertEquals('	5(0)
	left
		1(3)
		2(4)
		3(2)
	right
		7(1)
', $tree->display());

        $node = $tree->read(2);
        $this->assertEquals(2, $node->value());
        $this->assertEquals(4, $node->location());
    }

    /**
     * @return Tree
     * @throws \Exception
     */
    private function createTestTree()
    {
        $tree = new Tree();
        foreach([5,7,3,1,2] as $index => $value) {
            $node = new Node($index, $value);
            $tree->create($node);
        }
        return $tree;
    }


    /**
     * @return Tree
     * @throws \Exception
     */
    private function createDeepTestTree()
    {
        $tree = new Tree();
        foreach([5,7,3,1,2, 9, 15, 45, 11, 17, 14] as $index => $value) {
            $node = new Node($index, $value);
            $tree->create($node);
        }
        return $tree;
    }
}
