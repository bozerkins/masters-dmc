<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 04/03/2018
 * Time: 22:49
 */

namespace DataManagement;

use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\EntityRelationship\TableHelper;
use DataManagement\Model\EntityRelationship\TableIterator;
use DataManagement\Storage\FileStorage;
use PHPUnit\Framework\TestCase;

class MultipleTableTest extends TestCase
{
    /** @var Table */
    private $tableMain;
    /** @var Table */
    private $tableHelping;

    /**
     * @throws \Exception
     */
    public function setUp()
    {
        $tableMain = new Table(new FileStorage(tempnam('/tmp', 'test_table_1')));
        $tableMain->load($this->workingStructureMainTable());
        $tableMain->storage()->createAndFlush();
        $this->tableMain = $tableMain;

        $tableHelping = new Table(new FileStorage(tempnam('/tmp', 'test_table_2')));
        $tableHelping->load($this->workingStructureHelpingTable());
        $tableHelping->storage()->createAndFlush();
        $this->tableHelping = $tableHelping;

    }

    /**
     * @throws \Exception
     */
    public function testRelationalReadViaBulkReading()
    {
        $tableMain = $this->tableMain;
        $tableHelping = $this->tableHelping;

        $resultExpected = [];

        foreach(range(1,3) as $index) {
            $tableHelping->create(
                [
                    'ID' => $index,
                    'Title' => 'Type' . $index
                ]
            );
            $resultExpected['Type' . $index] = 0;
        }

        foreach(range(1,20) as $index) {
            $record = [
                'ID' => $index,
                'Profit' => $profit = rand(50,1000) / 10,
                'ProductType' => $type = rand(1,3),
                'ProductTypeReference' => 0
            ];
            $tableMain->create(
                $record
            );
            $resultExpected['Type' . $type] += $profit;
        }

        $result = [];
        $tableMain->iterate(function($record1) use ($tableHelping, &$result) {
            $productTypeRecord = null;
            $tableHelping->iterate(function($record2) use ($record1, &$productTypeRecord) {
                if ($record2['ID'] === $record1['ProductType']) {
                    $productTypeRecord = $record2;
                    return Table::OPERATION_READ_STOP;
                }
            });
            $result[] = [
                'Profit' => $record1['Profit'],
                'ProductType' => $productTypeRecord['Title']
            ];
        });

        $result = array_reduce($result, function($result, $record) {
            if (array_key_exists($record['ProductType'], $result) === false) {
                $result[$record['ProductType']] = 0;
            }
            $result[$record['ProductType']] += $record['Profit'];
            return $result;
        }, []);

        $this->assertEquals($resultExpected, $result);
    }

    /**
     * @throws \Exception
     */
    public function testRelationalReadViaIterators()
    {
        $tableMain = $this->tableMain;
        $tableHelping = $this->tableHelping;

        $resultExpected = [];

        foreach(range(1,3) as $index) {
            $tableHelping->create(
                [
                    'ID' => $index,
                    'Title' => 'Type' . $index
                ]
            );
            $resultExpected['Type' . $index] = 0;
        }

        foreach(range(1,20) as $index) {
            $record = [
                'ID' => $index,
                'Profit' => $profit = rand(50,1000) / 10,
                'ProductType' => $type = rand(1,3),
                'ProductTypeReference' => 0
            ];
            $tableMain->create(
                $record
            );
            $resultExpected['Type' . $type] += $profit;
        }

        $tableMain->reserve(Table::RESERVE_READ);
        $tableHelping->reserve(Table::RESERVE_READ);

        $result = [];
        $iteratorMain = $tableMain->newIterator();
        while($iteratorMain->endOfTable() === false) {
            $record = $iteratorMain->read();
            if ($record === null) {
                continue;
            }
            if (array_key_exists($record['ProductType'], $result) === false) {
                $result[$record['ProductType']] = 0;
            }
            $result[$record['ProductType']] += $record['Profit'];
        }

        $result2 = [];
        $iteratorHelping = $tableHelping->newIterator();
        while($iteratorHelping->endOfTable() === false) {
            $record = $iteratorHelping->read();
            if ($record === null) {
                continue;
            }
            if (array_key_exists($record['ID'], $result) === false) {
                continue;
            }
            $result2[$record['Title']] = $result[$record['ID']];
        }


        $tableMain->release();
        $tableHelping->release();

        $this->assertEquals($resultExpected, $result2);

    }

    /**
     * @throws \Exception
     */
    public function testRelationalReadViaReferences()
    {
        $tableMain = $this->tableMain;
        $tableHelping = $this->tableHelping;

        $resultExpected = [];

        foreach(range(1,3) as $index) {
            $tableHelping->create(
                [
                    'ID' => $index,
                    'Title' => 'Type' . $index
                ]
            );
            $resultExpected['Type' . $index] = 0;
        }

        foreach(range(1,20) as $index) {
            $record = [
                'ID' => $index,
                'Profit' => $profit = rand(50,1000) / 10,
                'ProductType' => $type = rand(1,3),
                'ProductTypeReference' => 0
            ];

            $tableHelping->iterate(function($relation, TableIterator $iterator) use (&$record) {
                if ($relation['ID'] === $record['ProductType']) {
                    $record['ProductTypeReference'] = $iterator->position() - 1;
                    return Table::OPERATION_READ_STOP;
                }
            });
            $tableMain->create(
                $record
            );
            $resultExpected['Type' . $type] += $profit;
        }

        $tableMain->reserve(Table::RESERVE_READ);
        $tableHelping->reserve(Table::RESERVE_READ);

        $iteratorHelping = $tableHelping->newIterator();
        $result3 = [];
        $tableMain->iterate(function($record) use (&$result3, $iteratorHelping) {
            $iteratorHelping->jump($record['ProductTypeReference']);
            $productType = $iteratorHelping->read();
            if (array_key_exists($productType['Title'], $result3) === false) {
                $result3[$productType['Title']] = 0;
            }
            $result3[$productType['Title']] += $record['Profit'];
        });

        $tableMain->release();
        $tableHelping->release();

        $this->assertEquals($resultExpected, $result3);
    }

    /**
     * @throws \Exception
     */
    public function testRelationalReadViaReferencesWithUpdates()
    {
        $tableMain = $this->tableMain;
        $tableHelping = $this->tableHelping;

        $resultExpected = [];

        foreach(range(1,3) as $index) {
            $tableHelping->create(
                [
                    'ID' => $index,
                    'Title' => 'Type' . $index
                ]
            );
            $resultExpected['Type' . $index] = 0;
        }

        foreach(range(1,20) as $index) {
            $record = [
                'ID' => $index,
                'Profit' => $profit = rand(50,1000) / 10,
                'ProductType' => $type = rand(1,3),
                'ProductTypeReference' => 0
            ];

            $tableMain->create(
                $record
            );

            $resultExpected['Type' . $type] += $profit;
        }

        $tableMain->update(
            function() {
                return Table::OPERATION_UPDATE_INCLUDE;
            },
            function($record) use ($tableHelping) {
                $update = [];
                $tableHelping->iterate(function($relation, TableIterator $iterator) use ($record, &$update) {
                    if ($relation['ID'] === $record['ProductType']) {
                        $update['ProductTypeReference'] = $iterator->position() - 1;
                        return Table::OPERATION_READ_STOP;
                    }
                });
                return $update;
            }
        );

        $tableMain->reserve(Table::RESERVE_READ);
        $tableHelping->reserve(Table::RESERVE_READ);

        $iteratorHelping = $tableHelping->newIterator();
        $result3 = [];
        $tableMain->iterate(function($record) use (&$result3, $iteratorHelping) {
            $iteratorHelping->jump($record['ProductTypeReference']);
            $productType = $iteratorHelping->read();
            if (array_key_exists($productType['Title'], $result3) === false) {
                $result3[$productType['Title']] = 0;
            }
            $result3[$productType['Title']] += $record['Profit'];
        });

        $tableMain->release();
        $tableHelping->release();

        $this->assertEquals($resultExpected, $result3);
    }

    /**
     * @return array
     */
    private function workingStructureMainTable()
    {
        return array (
            0 =>
                array (
                    'id' => 1,
                    'name' => 'ID',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
            1 =>
                array (
                    'id' => 2,
                    'name' => 'Profit',
                    'type' => TableHelper::COLUMN_TYPE_FLOAT,
                    'size' => 8,
                ),
            2 =>
                array (
                    'id' => 3,
                    'name' => 'ProductType',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
            3 =>
                array (
                    'id' => 4,
                    'name' => 'ProductTypeReference',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
        );
    }

    /**
     * @return array
     */
    private function workingStructureHelpingTable()
    {
        return array (
            0 =>
                array (
                    'id' => 1,
                    'name' => 'ID',
                    'type' => TableHelper::COLUMN_TYPE_INTEGER,
                    'size' => 4,
                ),
            1 =>
                array (
                    'id' => 2,
                    'name' => 'Title',
                    'type' => TableHelper::COLUMN_TYPE_STRING,
                    'size' => 30,
                ),
        );
    }
}
