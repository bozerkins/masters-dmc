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
        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
        $table->load(Node::structure(TableHelper::COLUMN_TYPE_INTEGER, TableHelper::getSizeByType(TableHelper::COLUMN_TYPE_INTEGER)));
        $table->storage()->createAndFlush();

        $table->reserve(Table::RESERVE_READ_AND_WRITE);

        $rootNode = new Node($table);
        $rootNode->createOnStorage(0);
        $tree = new Tree($table, $rootNode, 4);
        foreach ([5, 7, 3, 1, 2] as $index => $value) {
            $node = $tree->newNode($value);
            $tree->create($node);
        }

        $this->assertEquals('	5(1)
	left
		1(4)
		2(5)
		3(3)
	right
		7(2)
', $tree->draw());
        $node = $tree->read(2);
        $this->assertEquals(2, $node->value());
        $this->assertEquals(5, $node->location());

        $table->release();
    }
}
