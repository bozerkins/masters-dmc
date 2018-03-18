<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 03/03/2018
 * Time: 12:28
 */

namespace DataManagement;

use DataManagement\Model\Columnar\Table;
use DataManagement\Model\TableHelper;
use DataManagement\Storage\FileStorage;
use DataManagement\Storage\FileStorageCompressed;
use PHPUnit\Framework\TestCase;

class SingleTableCompressionTest extends TestCase
{
    private function newDirName()
    {
        return '/tmp/test_table_' . uniqid();
    }


    /**
     * @throws \Exception
     */
    public function testTableCreation()
    {
        $table = new Table($this->newDirName());
        $table->load($this->workingStructure());
        $table->makeDirectory();

        $this->assertEquals(true, is_dir($table->directory()));
        $this->assertEquals(true, is_writable($table->directory()));
    }

    /**
     * @throws \Exception
     */
    public function testTableReadWrite()
    {
        $table = new Table($this->newDirName());
        $table->load($this->workingStructure());
        $table->makeDirectory();

        $records = [];
        $records[] = [
            'ID' => 1,
            'Profit' => 15.22,
            'ProductTitle' => 'TestProduct',
            'Severity' => 12,
            'Date' => '2017-02-01'
        ];
        $records[] = [
            'ID' => 2,
            'Profit' => 11.2312,
            'ProductTitle' => 'SomeRealProduct',
            'Severity' => 2,
            'Date' => '2017-02-01'
        ];
        $records[] = [
            'ID' => 3,
            'Profit' => 105.22,
            'ProductTitle' => 'NotFake',
            'Severity' => 6,
            'Date' => '2017-02-05'
        ];

        $table->create($records);
        $table->merge('2017-02-01');

        $result = [];
        $table->iterate(
            ['Profit', 'Severity'],
            function($record) use (&$result) {
                $result[] = $record;
            }
        );
        $this->assertEquals(array (
            0 =>
                array (
                    'Profit' => 15.220000000000001,
                    'Severity' => 12,
                ),
            1 =>
                array (
                    'Profit' => 11.231199999999999,
                    'Severity' => 2,
                ),), $result);



        $table->merge('2017-02-05');

        $result = [];
        $table->iterate(
            ['ID', 'Profit', 'ProductTitle', 'Severity', 'Date'],
            function($record) use (&$result) {
                $result[] = $record;
            }
        );
        sort($records);
        sort($result);
        $this->assertEquals($records, $result);


        // check double merge
        $table->merge('2017-02-05');
        $result = [];
        $table->iterate(
            ['ID', 'Profit', 'ProductTitle', 'Severity', 'Date'],
            function($record) use (&$result) {
                $result[] = $record;
            }
        );
        sort($records);
        sort($result);
        $this->assertEquals($records, $result);
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
                    'partition' => 0
                ),
            1 =>
                array (
                    'id' => 2,
                    'name' => 'Profit',
                    'type' => 2,
                    'size' => 8,
                    'partition' => 0
                ),
            2 =>
                array (
                    'id' => 3,
                    'name' => 'ProductTitle',
                    'type' => 3,
                    'size' => 255,
                    'partition' => 0
                ),
            3 =>
                array (
                    'id' => 4,
                    'name' => 'Severity',
                    'type' => 1,
                    'size' => 4,
                    'partition' => 0
                ),
            4 =>
                array (
                    'id' => 5,
                    'name' => 'Date',
                    'type' => TableHelper::COLUMN_TYPE_STRING,
                    'size' => 11,
                    'partition' => 1
                ),
        );
    }
}
