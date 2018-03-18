<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 18/03/2018
 * Time: 16:05
 */

namespace DataManagement\Storage;

interface FileStorageInterface
{
    /**
     * @throws \Exception
     */
    public function create();

    /**
     * @throws \Exception
     */
    public function createAndFlush();

    /**
     * @return resource
     */
    public function handle();

    /**
     * @return string
     */
    public function file();

    /**
     * @param $mode
     * @throws \Exception
     */
    public function open($mode);

    /**
     * @throws \Exception
     */
    public function close();

    /**
     * @see fread
     * @param null $length
     * @return bool|string
     */
    public function read($length);

    /**
     * @see fwrite
     * @param $string
     * @param null $length
     */
    public function write($string, $length = null);

    /**
     * @see fseek
     * @param $offset
     * @param int $whence
     */
    public function seek($offset, $whence = SEEK_SET);

    /**
     * @see ftell
     * @return bool|int
     */
    public function tell();

    /**
     * @return bool
     */
    public function eof();

    /**
     * @param $operation
     * @throws \Exception
     */
    public function acquire($operation);

    /**
     * @throws \Exception
     */
    public function release();
}