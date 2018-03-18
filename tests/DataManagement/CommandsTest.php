<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 03/03/2018
 * Time: 12:28
 */

namespace DataManagement;

use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\Index\Node;
use DataManagement\Model\Index\Tree;
use DataManagement\Model\TableHelper;
use DataManagement\Storage\FileStorage;
use PHPUnit\Framework\TestCase;

class CommandsTest extends TestCase
{
    /**
     * @throws \Exception
     */
    public function testTableCreation()
    {
        $instructionsFile = __DIR__ . '/instructions/example01.php';
        shell_exec(__DIR__ . '/../../bin/dmc dmc:er:table-create ' . $instructionsFile);
        $table = Table::newFromInstructionsFile($instructionsFile);
        $this->assertEquals(true, file_exists($table->storage()->file()));
    }

    /**
     * @throws \Exception
     */
    public function testTableDrop()
    {
        $instructionsFile = __DIR__ . '/instructions/example01.php';
        shell_exec(__DIR__ . '/../../bin/dmc dmc:er:table-drop ' . $instructionsFile);
        $table = Table::newFromInstructionsFile($instructionsFile);
        $this->assertEquals(false, file_exists($table->storage()->file()));
    }

    /**
     * @throws \Exception
     */
    public function testTableSize()
    {
        $instructionsFile = __DIR__ . '/instructions/example01.php';
        $table = Table::newFromInstructionsFile($instructionsFile);
        $table->storage()->createAndFlush();
        foreach(range(1,20) as $index) {
            $record = [
                'ID' => $index,
                'Profit' => $profit = rand(50,1000) / 10,
                'ProductType' => $type = rand(1,3),
                'ProductTypeReference' => 0
            ];
            $table->create($record);
        }
        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile);
        $this->assertEquals("20\n", $output);

        $table->delete(function($record) {
            if ($record['ID'] === 2 || $record['ID'] === 3) {
                return Table::OPERATION_DELETE_INCLUDE;
            }
        });

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile);
        $this->assertEquals("20\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile . ' --active');
        $this->assertEquals("18\n", $output);
    }

    /**
     * @throws \Exception
     */
    public function testTableOptimize()
    {
        $instructionsFile = __DIR__ . '/instructions/example01.php';
        $table = Table::newFromInstructionsFile($instructionsFile);
        $table->storage()->createAndFlush();
        foreach(range(1,20) as $index) {
            $record = [
                'ID' => $index,
                'Profit' => $profit = rand(50,1000) / 10,
                'ProductType' => $type = rand(1,3),
                'ProductTypeReference' => 0
            ];
            $table->create($record);
        }
        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile);
        $this->assertEquals("20\n", $output);

        $table->delete(function($record) {
            if ($record['ID'] === 2 || $record['ID'] === 3) {
                return Table::OPERATION_DELETE_INCLUDE;
            }
        });

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile);
        $this->assertEquals("20\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile . ' --active');
        $this->assertEquals("18\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:table-optimize ' . $instructionsFile . ' --analyze');
        $this->assertEquals("Total records: 20\nTotal active records: 18\nWaste percentage: 90%\noptimization not required\n", $output);

        $table->delete(function($record) {
            if ($record['ID'] > 10) {
                return Table::OPERATION_DELETE_INCLUDE;
            }
        });

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:table-optimize ' . $instructionsFile . ' --analyze');
        $this->assertEquals("Total records: 20\nTotal active records: 8\nWaste percentage: 40%\noptimization required\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:table-optimize ' . $instructionsFile);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile);
        $this->assertEquals("8\n", $output);

        $output = shell_exec(__DIR__ . '/../../bin/dmc dmc:er:records ' . $instructionsFile . ' --active');
        $this->assertEquals("8\n", $output);
    }
}
