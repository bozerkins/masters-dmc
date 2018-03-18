<?php

return [
    'location' => '/tmp/dmc_columnar_test_file_1312315878md172sn_' . shell_exec('git log --pretty=format:\'%h\' -n 1'),
    'structure' => [
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
                    'name' => 'Date',
                    'type' => 3,
                    'size' => 11,
                    'partition' => 1
                ),
    ]
];