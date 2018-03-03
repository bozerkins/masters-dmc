<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 2/28/2018
 * Time: 11:21 PM
 */

namespace DataManagement;

use DataManagement\Storage\FileStorage;
use PHPUnit\Framework\TestCase;

/**
 * Class DefaultTest
 * @package DataManagement
 *
 * Proof of concept tests
 */
class DefaultTest extends TestCase
{
    public function testNothing()
    {
        $this->assertEquals(1,1);

        $format = 'f*';
        $record = [15,12];
        $bin = pack($format, ...$record);
//        echo strlen($bin);
//        echo PHP_EOL;
//        print_r(
//            unpack($format, $bin)
//        );
    }

    public function testBasicTableConcepts()
    {
        $filename = '/tmp/storage_test';
        // clear the file
        fclose(fopen($filename, 'w'));

        /*
         * Table with 3 columns
         * 1. Integer value - ID
         * 2. Float value - Price
         * 3. String (size 20) - Name
         */

        $records = [];
        $item = [];
        $item[] = 15;
        $item[] = 22.15;
        $item[] = 'Helmet';
        $records[] = $item;
        $item = [];
        $item[] = 2;
        $item[] = 111.15;
        $item[] = 'Gloves & More';
        $records[] = $item;

        $handler = fopen($filename, 'wb');
        $writeFormat = 'ifZ20';
        $writeSize = 28;

        foreach($records as $record) {
            $pack = pack($writeFormat, ...$record);
            fwrite($handler, $pack, $writeSize);
        }
        fclose($handler);

        $handler = fopen($filename, 'rb');
        $readFormat = 'icol1/fcol2/Z20col3';
        $readSize = 28;
        while(!feof($handler)) {
            $read = fread($handler, $readSize);
            if (strlen($read) != $readSize) {
                continue;
            }
            dump($read);
            $unpack = unpack($readFormat, $read);
            dump($unpack);
        }
//        var_dump(unpack($format, $read));
//        $this->assertEquals(1, 1);
    }
}