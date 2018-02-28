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
    private $folder;

    const OPEN_TYPE_SHARED = 1;
    const OPEN_TYPE_EXCLUSIVE = 2;

    /**
     * FileStorage constructor.
     * @param string $folder
     */
    public function __construct(string $folder)
    {
        $this->folder = rtrim($folder, '/') . '/';
    }

    /**
     * @param $file
     * @return bool|resource
     * @throws \Exception
     */
    public function openShared($file)
    {
        $handle = $this->open($file, 'r');
        $this->acquire($handle, LOCK_SH);
        return $handle;
    }

    /**
     * @param string $file
     * @return bool|resource
     * @throws \Exception
     */
    public function openExclusive(string $file)
    {
        $handle = $this->open($file, 'r');
        $this->acquire($handle, LOCK_EX);
        return $handle;
    }

    /**
     * @param $handle
     * @throws \Exception
     */
    public function close($handle)
    {
        if (false === fclose($handle)) {
            throw new \Exception('fail to close');
        }
    }

    /**
     * @param string $file
     * @param $mode
     * @return bool|resource
     * @throws \Exception
     */
    private function open(string $file, $mode)
    {
        $handle = fopen($this->folder . $file, $mode);
        if (false === $handle) {
            throw new \Exception('fail to open');
        }
        return $handle;
    }

    /**
     * @param $handle
     * @param $operation
     * @throws \Exception
     */
    public function acquire($handle, $operation)
    {
        if (false === flock($handle, $operation)) {
            throw new \Exception('fail to lock');
        }
    }

    /**
     * @param $handle
     * @throws \Exception
     */
    public function release($handle)
    {
        if (flock($handle, LOCK_UN) === false) {
            throw new \Exception('fail to unlock');
        }
    }
}