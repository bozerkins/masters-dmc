<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 03/03/2018
 * Time: 12:28
 */

namespace DataManagement;

use DataManagement\Model\Columnar\Table;
use PHPUnit\Framework\TestCase;

class CommandsColumnarTest extends TestCase
{
    /**
     * @throws \Exception
     */
    public function testTableDrop()
    {
        $instructionsFile = __DIR__ . '/instructions/example02.php';
        shell_exec(__DIR__ . '/../../bin/dmc dmc:col:table-drop ' . $instructionsFile);
        $table = Table::newFromInstructionsFile($instructionsFile);
        $this->assertEquals(false, is_dir($table->directory()));
    }

    /**
     * @throws \Exception
     */
    public function testTableColumnarSize()
    {
        $instructionsFile = __DIR__ . '/instructions/example02.php';
        $table = Table::newFromInstructionsFile($instructionsFile);
        $table->makeDirectoryAndFlush();

        $records = [];
        foreach(range(1,50) as $index) {
            $records[] = [
                'ID' => $index,
                'Date' => $this->randomDate('2017-01-02', '2017-01-10')
            ];
        }
        $table->create($records);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:records ' . $instructionsFile);
        $this->assertEquals("0\n", $output);

        foreach($table->partitions() as $partition) {
            $table->merge($partition);
        }
        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:records ' . $instructionsFile);
        $this->assertEquals("50\n", $output);
    }

    private function randomDate($start_date, $end_date)
    {
        // Convert to timetamps
        $min = strtotime($start_date);
        $max = strtotime($end_date);

        // Generate random number using above bounds
        $val = rand($min, $max);

        // Convert back to desired date format
        return date('Y-m-d', $val);
    }

    /**
     * @throws \Exception
     */
    public function testTableColumnarMerge()
    {
        $instructionsFile = __DIR__ . '/instructions/example02.php';
        $table = Table::newFromInstructionsFile($instructionsFile);
        $table->makeDirectoryAndFlush();

        $records = [];
        foreach(range(1,50) as $index) {
            $records[] = [
                'ID' => $index,
                'Date' => $this->randomDate('2017-01-02', '2017-01-10')
            ];
        }
        $table->create($records);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:records ' . $instructionsFile);
        $this->assertEquals("0\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:table-merge ' . $instructionsFile);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:records ' . $instructionsFile);
        $this->assertEquals("50\n", $output);

        foreach(range(101,110) as $index) {
            $records[] = [
                'ID' => $index,
                'Date' => '2017-05-02'
            ];
        }
        $table->create($records);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:records ' . $instructionsFile);
        $this->assertEquals("50\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:table-merge ' . $instructionsFile . ' --partition=2017-05-02');

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:records ' . $instructionsFile);
        $this->assertEquals("60\n", $output);
    }

    /**
     * @throws \Exception
     */
    public function testTableListPartitions()
    {
        $instructionsFile = __DIR__ . '/instructions/example02.php';
        $table = Table::newFromInstructionsFile($instructionsFile);
        $table->makeDirectoryAndFlush();

        $records = [];
        $partitions = [];
        foreach(range(1,50) as $index) {
            $records[] = [
                'ID' => $index,
                'Date' => $partitions[] = $this->randomDate('2017-01-02', '2017-01-10')
            ];
        }
        $partitions = array_values(array_unique($partitions));
        $table->create($records);

        foreach($table->partitions() as $partition) {
            $table->merge($partition);
        }

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:col:table-partitions ' . $instructionsFile);
        $outputArr = explode(PHP_EOL, trim($output));
        sort($outputArr);
        sort($partitions);
        $this->assertEquals($partitions, $outputArr);
    }
}
