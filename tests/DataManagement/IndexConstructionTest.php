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
    public function testIndexFormationForUniqueValuesSet()
    {
//        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
//        $table->load($this->workingStructure());
//        $table->storage()->createAndFlush();

        $values = $this->getValuesUnique();
        $values = array_values(array_splice($values, 0, 20));

        $tree = new Tree();
        foreach($values as $index => $value) {
            $node = new Node($index, $value);
            $tree->add($node);
        }

        $node = $tree->find(25);
        dump([$node->value(), $node->location()]);

        echo 'tree:', PHP_EOL;
        echo $tree->display();
        echo PHP_EOL;

//        dump($tree);

        $this->assertEquals(1,1);


    }

    private function getValuesUnique()
    {
        return array_values(array_unique($this->getValues()));
    }

    private function getValues()
    {
        return array (
            0 => 19,
            1 => 26,
            2 => 13,
            3 => 20,
            4 => 11,
            5 => 4,
            6 => 18,
            7 => 21,
            8 => 27,
            9 => 7,
            10 => 17,
            11 => 14,
            12 => 11,
            13 => 9,
            14 => 11,
            15 => 18,
            16 => 24,
            17 => 27,
            18 => 25,
            19 => 2,
        );
    }
}
