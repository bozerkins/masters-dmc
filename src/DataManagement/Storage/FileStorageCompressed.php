<?php
/**
 * Created by PhpStorm.
 * User: Bogdans
 * Date: 2/28/2018
 * Time: 11:52 PM
 */

namespace DataManagement\Storage;


class FileStorageCompressed implements FileStorageInterface
{
    /** @var string */
    private $file;
    /** @var resource */
    private $handle;
    /** @var resource */
    private $lockHandle;
    /** @var int */
    private $compress;

    /**
     * FileStorage constructor.
     * @param string $file
     * @param int $compress level to which to compress
     * @see gzcompress() for details on compression level
     */
    public function __construct(string $file, int $compress)
    {
        $this->file = $file . '.gz';
        $this->compress = $compress;
    }

    /**
     * @throws \Exception
     */
    public function create()
    {
        if (false === is_dir(dirname($this->file))) {
            @mkdir(dirname($this->file));
        }
        if (false === file_exists($this->file)) {
            if (false === touch($this->file)) {
                throw new \Exception('failed to create the file');
            }
        }
    }

    /**
     * @return bool
     */
    public function present()
    {
        return is_file($this->file);
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
            $handler = gzopen($this->file, 'w');
            gzclose($handler);
        }
    }

    /**
     * @return resource
     */
    public function handle()
    {
        return $this->handle;
    }

    /**
     * @return string
     */
    public function file()
    {
        return $this->file;
    }

    /**
     * @param $mode
     * @throws \Exception
     */
    public function open($mode)
    {
        $handle = gzopen($this->file, $mode);
        if (false === $handle) {
            throw new \Exception('fail to open');
        }
        $this->handle = $handle;
    }

    /**
     * @throws \Exception
     */
    public function close()
    {
        if (false === gzclose($this->handle)) {
            throw new \Exception('fail to close');
        }
    }

    /**
     * @see fread
     * @param null $length
     * @return bool|string
     */
    public function read($length)
    {
        return gzread($this->handle, $length);
    }

    /**
     * @see fwrite
     * @param $string
     * @param null $length
     */
    public function write($string, $length = null)
    {
        gzwrite($this->handle, $string, $length);
    }

    /**
     * @see fseek
     * @param $offset
     * @param int $whence
     */
    public function seek($offset, $whence = SEEK_SET)
    {
        gzseek($this->handle, $offset, $whence );
    }

    /**
     * @see ftell
     * @return bool|int
     */
    public function tell()
    {
        return gztell($this->handle);
    }

    /**
     * @return bool
     */
    public function eof()
    {
        return gzeof($this->handle);
    }

    /**
     * @param $operation
     * @throws \Exception
     */
    public function acquire($operation)
    {
        $this->lockHandle = fopen($this->file . '.lock', 'w');
        if (false === $this->lockHandle) {
            throw new \Exception('failed to create lock handler');
        }
        if (false === flock($this->lockHandle, $operation)) {
            throw new \Exception('fail to lock');
        }
    }

    /**
     * @throws \Exception
     */
    public function release()
    {
        if (false === $this->lockHandle) {
            throw new \Exception('cannot unlock without active handle');
        }
        if (flock($this->lockHandle, LOCK_UN) === false) {
            throw new \Exception('fail to unlock');
        }
    }

    /**
     * @throws \Exception
     */
    public function remove()
    {
        if (false === unlink($this->file)) {
            throw new \Exception('failed to remove the file');
        }
    }

    /**
     * @param string $destination
     * @throws \Exception
     */
    public function move(string $destination)
    {
        if (false === rename($this->file, $destination)) {
            throw new \Exception('failed to move the file');
        }
    }
}