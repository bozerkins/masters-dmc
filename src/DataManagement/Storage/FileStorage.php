<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 2/28/2018
 * Time: 11:52 PM
 */

namespace DataManagement\Storage;


class FileStorage
{
    /** @var string */
    private $file;
    /** @var resource */
    private $handle;

    /**
     * FileStorage constructor.
     * @param string $file
     */
    public function __construct(string $file)
    {
        $this->file = $file;
    }

    /**
     * @throws \Exception
     */
    public function create()
    {
        if (false === file_exists($this->file)) {
            if (false === touch($this->file)) {
                throw new \Exception('failed to create the file');
            }
        }
    }

    /**
     * @throws \Exception
     */
    public function createAndFlush()
    {
        if (false === file_exists($this->file)) {
            if (false === touch($this->file)) {
                throw new \Exception('failed to create the file');
            }
        } else {
            $handler = fopen($this->file, 'w');
            fclose($handler);
        }
    }

    public function handle()
    {
        return $this->handle;
    }

    public function file()
    {
        return $this->file;
    }

    /**
     * @throws \Exception
     */
    public function close()
    {
        if (false === fclose($this->handle)) {
            throw new \Exception('fail to close');
        }
    }

    /**
     * @param $mode
     * @return bool|resource
     * @throws \Exception
     */
    public function open($mode)
    {
        $handle = fopen($this->file, $mode);
        if (false === $handle) {
            throw new \Exception('fail to open');
        }
        $this->handle = $handle;
    }

    /**
     * @param $operation
     * @throws \Exception
     */
    public function acquire($operation)
    {
        if (false === flock($this->handle, $operation)) {
            throw new \Exception('fail to lock');
        }
    }

    /**
     * @throws \Exception
     */
    public function release()
    {
        if (flock($this->handle, LOCK_UN) === false) {
            throw new \Exception('fail to unlock');
        }
    }
}