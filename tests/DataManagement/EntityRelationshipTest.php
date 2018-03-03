<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 03/03/2018
 * Time: 12:28
 */

namespace DataManagement;

use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Storage\FileStorage;
use PHPUnit\Framework\TestCase;

class EntityRelationshipTest extends TestCase
{
    /**
     * @throws \Exception
     */
    public function testTableStructure()
    {
        $table = new Table(new FileStorage(':memory'));
        $table->addColumn('ID', Table::COLUMN_TYPE_INTEGER);
        $table->addColumn('Profit', Table::COLUMN_TYPE_FLOAT);
        $table->addColumn('ProductTitle', Table::COLUMN_TYPE_STRING);
        $table->addColumn('Severity', Table::COLUMN_TYPE_INTEGER);
        $structure = $table->structure();
        $this->assertEquals($structure, $this->workingStructure());

        $table = new Table(new FileStorage(':memory'));
        $table->load($this->workingStructure());
        $structure = $table->structure();
        $this->assertEquals($structure, $this->workingStructure());
    }

    /**
     * @throws \Exception
     */
    public function testTableCreation()
    {
        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
        $table->load($this->workingStructure());
        $table->storage()->createAndFlush();

        $this->assertEquals(true, is_file($table->storage()->file()));
        $this->assertEquals(true, is_writable($table->storage()->file()));
    }

    /**
     * @throws \Exception
     */
    public function testTableReadWrite()
    {
        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
        $table->load($this->workingStructure());
        $table->storage()->createAndFlush();

        $records = [];
        $records[] = [
            'ID' => 1,
            'Profit' => 15.22,
            'ProductTitle' => 'TestProduct',
            'Severity' => 12
        ];
        $records[] = [
            'ID' => 2,
            'Profit' => 11.2312,
            'ProductTitle' => 'SomeRealProduct',
            'Severity' => 2
        ];
        $records[] = [
            'ID' => 3,
            'Profit' => 105.22,
            'ProductTitle' => 'NotFake',
            'Severity' => 6
        ];

        foreach($records as $record) {
            $table->create($record);
        }

        $result = [];
        $table->iterate(function($record) use (&$result) {
            $result[] = $record;
        });
        $this->assertEquals($records, $result);

        $result = $table->read(function($record) {
            return Table::OPERATION_READ_INCLUDE;
        });
        $this->assertEquals($records, $result);

        $result = $table->read(function($record) {
            static $found = false;
            if ($record['ID'] === 2) {
                $found = true;
                return Table::OPERATION_READ_INCLUDE;
            }
            if ($found) {
                return Table::OPERATION_READ_STOP;
            }
        });
        $this->assertEquals($records[1], $result[0]);
        $this->assertEquals(1, count($result));

        $result = $table->read(function($record) {
            if ($record['ID'] === 3) {
                return Table::OPERATION_READ_INCLUDE_AND_STOP;
            }
        });
        $this->assertEquals($records[2], $result[0]);
        $this->assertEquals(1, count($result));

    }

    /**
     * @throws \Exception
     *
     */
    public function testTableUpdate()
    {
        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
        $table->load($this->workingStructure());
        $table->storage()->createAndFlush();

        $records = [];
        $records[] = [
            'ID' => 1,
            'Profit' => 15.22,
            'ProductTitle' => 'TestProduct',
            'Severity' => 12
        ];
        $records[] = [
            'ID' => 2,
            'Profit' => 11.2312,
            'ProductTitle' => 'SomeRealProduct',
            'Severity' => 2
        ];
        $records[] = [
            'ID' => 3,
            'Profit' => 105.22,
            'ProductTitle' => 'NotFake',
            'Severity' => 6
        ];

        foreach($records as $record) {
            $table->create($record);
        }

        $table->update(
            function($record) {
                if ($record['Severity'] > 3) {
                    return Table::OPERATION_UPDATE_INCLUDE;
                }
            },
            function($record) {
                return [
                    'Profit' => $record['Profit'] + 10
                ];
            }
        );

        $result = $table->read(function($record) {
            return Table::OPERATION_READ_INCLUDE;
        });
        $this->assertEquals(
            array_column(
                array_map(
                    function ($record) {
                        if ($record['Severity'] > 3) {
                            $record['Profit'] += 10;
                        }
                        return $record;
                    },
                    $records
                ),
                'Profit'
            ),
            array_column($result, 'Profit')
        );
    }

    /**
     * @throws \Exception
     */
    public function testTableDelete()
    {
        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
        $table->load($this->workingStructure());
        $table->storage()->createAndFlush();

        $records = [];
        $records[] = [
            'ID' => 1,
            'Profit' => 15.22,
            'ProductTitle' => 'TestProduct',
            'Severity' => 12
        ];
        $records[] = [
            'ID' => 2,
            'Profit' => 11.2312,
            'ProductTitle' => 'SomeRealProduct',
            'Severity' => 2
        ];
        $records[] = [
            'ID' => 3,
            'Profit' => 105.22,
            'ProductTitle' => 'NotFake',
            'Severity' => 6
        ];
        $records[] = [
            'ID' => 4,
            'Profit' => 205.22,
            'ProductTitle' => 'DefinitelyNotFake',
            'Severity' => 23
        ];

        foreach($records as $record) {
            $table->create($record);
        }

        $table->delete(function($record) {
            if ($record['ID'] === 2 || $record['ID'] === 3) {
                return Table::OPERATION_DELETE_INCLUDE;
            }
        });

        $result = $table->read(function(){
            return Table::OPERATION_READ_INCLUDE;
        });

        $this->assertEquals([$records[0], $records[3]], $result);
    }

    /**
     * @throws \Exception
     */
    public function testTableOptimize()
    {
        $table = new Table(new FileStorage(tempnam('/tmp', 'test_table_')));
        $table->load($this->workingStructure());
        $table->storage()->createAndFlush();

        $records = [];
        $records[] = [
            'ID' => 1,
            'Profit' => 15.22,
            'ProductTitle' => 'TestProduct',
            'Severity' => 12
        ];
        $records[] = [
            'ID' => 2,
            'Profit' => 11.2312,
            'ProductTitle' => 'SomeRealProduct',
            'Severity' => 2
        ];
        $records[] = [
            'ID' => 3,
            'Profit' => 105.22,
            'ProductTitle' => 'NotFake',
            'Severity' => 6
        ];
        $records[] = [
            'ID' => 4,
            'Profit' => 205.22,
            'ProductTitle' => 'DefinitelyNotFake',
            'Severity' => 23
        ];

        foreach($records as $record) {
            $table->create($record);
        }

        $table->delete(function($record) {
            if ($record['ID'] === 2 || $record['ID'] === 3) {
                return Table::OPERATION_DELETE_INCLUDE;
            }
        });

        $result = $table->read(function(){
            return Table::OPERATION_READ_INCLUDE;
        });

        $this->assertEquals([$records[0], $records[3]], $result);

        clearstatcache();
        $fsize = filesize($table->storage()->file());

        $table->optimize();

        clearstatcache();
        $fsize = filesize($table->storage()->file());
    }

    /**
     * @return array
     */
    private function workingStructure()
    {
        return array (
            0 =>
                array (
                    'id' => 1,
                    'name' => 'ID',
                    'type' => 1,
                    'size' => 4,
                ),
            1 =>
                array (
                    'id' => 2,
                    'name' => 'Profit',
                    'type' => 2,
                    'size' => 8,
                ),
            2 =>
                array (
                    'id' => 3,
                    'name' => 'ProductTitle',
                    'type' => 3,
                    'size' => 255,
                ),
            3 =>
                array (
                    'id' => 4,
                    'name' => 'Severity',
                    'type' => 1,
                    'size' => 4,
                ),
        );
    }
}
