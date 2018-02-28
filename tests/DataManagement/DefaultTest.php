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

class DefaultTest extends TestCase
{
    public function testNothing()
    {
        $this->assertEquals(1,1);
    }

    public function testFileStorageInitialization()
    {
        $storage = new FileStorage();
    }
}