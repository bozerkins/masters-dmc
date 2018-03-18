<?php

return [
    'location' => '/tmp/dmc_test_file_1312315878md172sn_' . shell_exec('git log --pretty=format:\'%h\' -n 1'),
    'structure' => [
        0 =>
            array (
                'id' => 1,
                'name' => 'ID',
                'type' => 1,
                'size' => 4,
            ),
        1 =>
            array (
                'id' => 2,
                'name' => 'Profit',
                'type' => 2,
                'size' => 8,
            ),
        2 =>
            array (
                'id' => 3,
                'name' => 'ProductType',
                'type' => 1,
                'size' => 4,
            ),
        3 =>
            array (
                'id' => 4,
                'name' => 'ProductTypeReference',
                'type' => 1,
                'size' => 4,
            ),
    ]
];