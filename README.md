# Data Management Components

A small library implementing some of the components for data management.

## Supported paradigms
* Entity Relationship Tables
* B-Tree index construction (based on ER Tables)
* Columnar Tables

## Supported data types

Each component supports the following data types
* Signed Integer, 4bytes size
* Signed Decimal, 8bytes size
* String, fixed size, 1char = 1byte, 1byte reserved to mark the end of string

## Table definition

All tables are defined in the special instructions files and then initialized in code

### Instructions file example for ER Table
```php
<?php

return [
    # location of the table file
    'location' => '~/storage/my-table',
    # table structure
    'structure' => [
        array (
            'name' => 'ID',
            'type' => 1,   # integer
            'size' => 4,   # size of integer
        ),
        array (
            'name' => 'Profit',
            'type' => 2,   # decimal
            'size' => 8,   # size of decimal
        ),
        array (
            'name' => 'ProductTypeReference',
            'type' => 1,   # integer
            'size' => 4,   # size of integer
        ),
        array (
            'name' => 'ProductTitle',
            'type' => 3,   # string
            'size' => 100, # size of string
        ),
    ]
];
```

Important notes for table definition
* changing table structure currently is not supported
* column names can be changed
* column order in the structure cannot be changed
* string takes 1 additional byte for end of string definition, which means that size of 5 allows 4 symbols to be written

### Instructions file example generation for ER Table

Instructions file can be easily generated using the library means

```php
<?php

use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\TableHelper;

$table = new Table();
$table->addColumn('ID', TableHelper::COLUMN_TYPE_INTEGER);
$table->addColumn('Profit', TableHelper::COLUMN_TYPE_FLOAT);
$table->addColumn('ProductType', TableHelper::COLUMN_TYPE_INTEGER);
$table->addColumn('ProductTypeReference', TableHelper::COLUMN_TYPE_INTEGER);
$table->addColumn('ProductTitle', TableHelper::COLUMN_TYPE_STRING, 100);

$structure = var_export($table->structure(), true);
$location = '~/storage/my-table';

$instructions = <<<EOT
<?php
return [
    'location' => {$location},
    'structure' => {$structure}
];
EOT;
$instructionFileDestination = '/project-root/my-table-instruction.php';
# create the file
file_put_contents($instructions, $instructionFileDestination);
# make it writable by everyone
chmod($instructionFileDestination, 0777);
```

### Instructions file example for Columnar Table

Columnar table implements partitioning by default. 

Difference in data definition from ER Table 
* you need to define exactly one partitioning column
* location for Columnar table will be a folder, not a file

```php
<?php

return [
    'location' => '~/storage/my-table',
    'structure' => [
            array (
                'name' => 'ID',
                'type' => 1,
                'size' => 4,
                'partition' => 0
            ),
            array (
                'name' => 'Date',
                'type' => 3,
                'size' => 11,
                'partition' => 1
            ),
    ]
];
```

### Creating a table from instructions

```php
<?php

# ER Table
$instructionFileDestinationER = '/project-root/my-table-instruction-er.php';
$erTable = \DataManagement\Model\EntityRelationship\Table::newFromInstructionsFile($instructionFileDestinationER);

# Columnar table
$instructionFileDestinationColumnar = '/project-root/my-table-instruction-columnar.php';
$columnarTable = \DataManagement\Model\Columnar\Table::newFromInstructionsFile($instructionFileDestinationColumnar);
```

## Entity Relations Table functionality

ER Tables implement CRUD operations, Table locking (instead of transactions) and Relationships.

### Operating ER Table

To perform CRUD operations on an ER Table a special TableIterator component was implement.

TableIterator allows jumping between records, getting table statistics and performing CRUD operations.
Each TableIterator action (CRUD) moves the pointer to the next record or end of file.

Simplified Table interface internally uses TableIterator.

#### Iterator create operation

```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# reserve the table
$table->reserve(Table::RESERVE_WRITE);
# get iterator
$iterator = $table->newIterator();
# jump to the end of file
$iterator->end();
# create record
$iterator->create(['ID' => 1, 'Profit' => 2.11, 'ProductTypeReference' => 0, 'ProductTitle' => 'My first product, yay!']);
# release the table
$table->release();
``` 

Important notes:
* NULL type is not supported
* when iterator is not at the end of file, create operation will replace the file
* without the reserving the table operation will not be possible

#### Table create operation

```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# create
$table->create(['ID' => 1, 'Profit' => 2.11, 'ProductTypeReference' => 0, 'ProductTitle' => 'My first product, yay!']);
``` 
Important notes:
* lock are automatically acquired
* it is possible to lock the table before running create manually, but then you need to manually release it as well

#### Iterator update operation

```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# reserve the table
$table->reserve(Table::RESERVE_WRITE);
# get iterator
$iterator = $table->newIterator();
# jump to the record index
$iterator->jump(5); 
# update record
$iterator->update(['ProductTitle' => 'My first product update!']);
# release the table
$table->release();
``` 

Important notes:
* if the record is deleted but not removed completely, it will still be updated
* jump method allows jumping to any record in the table

#### Table update operation

```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# find and update the table
$table->update(
    # search the record
    function(array $record) {
        if ($record['ID'] === 5) {
            return Table::OPERATION_UPDATE_INCLUDE_AND_STOP;
        }
    },
    # update each record that's included
    function(array $record) {
        return ['ProductTitle' => 'My first product update!'];
    }
);
``` 

#### Iterator read operation
```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# reserve the table
$table->reserve(Table::RESERVE_READ);
# get iterator
$iterator = $table->newIterator();
# jump to the record index
$iterator->jump(5); 
# read record
$record = $iterator->read();
# read all the records in the table
$result = [];
# reset pointer
$iterator->jump(0);
# loop over table
while ($iterator->endOfTable() === false) {
    # read the record
    $record = $iterator->read();
    # check if it's not deleted
    if ($record === null) {
        continue;
    }
    # add to result
    $result[] = $record;
}
# release the table
$table->release();
``` 

Important notes:
* read operation (as any) moves the pointer to the next record
* if the record was deleted, then NULL is returned from "TableIterator::read" method

#### Table read operation
```php
<?php
use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\EntityRelationship\TableIterator;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# read all the table
$result = $table->read(function(){
    return Table::OPERATION_READ_INCLUDE;
});
# read the record with specific ID
$result = $table->read(function(array $record){
    if ($record['ID'] === 5) {
        return Table::OPERATION_READ_INCLUDE_AND_STOP;
    }
});
# read only first two record
$result = [];
$table->iterate(function(array $record) use (&$result) {
    $result[] = $record;
    if (count($result) === 2) {
        return Table::OPERATION_READ_STOP;
    }
});
# read id and pointer of all the records
$result = [];
$table->iterate(function(array $record, TableIterator $iterator) use (&$result) {
    $result[] = ['id' => $record['ID'], 'pointer' => $iterator->position()];
});
```

Important notes:
* first parameter to the table callback is record
* second parameter is TableIterator (this provides more control over iteraton) 

#### Iterator delete operation
```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# reserve the table
$table->reserve(Table::RESERVE_WRITE);
# get iterator
$iterator = $table->newIterator();
# jump to the record index
$iterator->jump(5); 
# update record
$iterator->delete();
# release the table
$table->release();
``` 

Important notes:
* Record is marked as deleted, but do not disappear from the file (deleting does not change table file size)
* To physically delete records from the file run ```bin/dmc dmc:er:table-optimize <table-instruction-file>``` command

#### Table delete operation

```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# find and update the table
$table->delete(
    # search the record
    function(array $record) {
        if ($record['ID'] === 5) {
            return Table::OPERATION_DELETE_INCLUDE_AND_STOP;
        }
    }
);
```

#### Iterator operation combinations

TableIterator provides more sophisticated ways of working with the Table.

Example of search and update operations
```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);
# reserve the table
$table->reserve(Table::RESERVE_READ_AND_WRITE);
# get iterator
$iterator = $table->newIterator();
# reset pointer
$iterator->jump(0);
# loop over table
while ($iterator->endOfTable() === false) {
    # read the record
    $record = $iterator->read();
    # check if it's not deleted
    if ($record === null) {
        continue;
    }
    # check if record found
    if ($record['ID'] === 5) {
        # rewind pointer to the record location
        $iterator->rewind(1);
        # update
        $iterator->update(['ProductTitle' => 'Updating in the iterator']);
        # exit the loop
        break;
    }
}
# release the table
$table->release();
```  
### Relations

Relations are implemented as simple position pointers, e.g. integer value which contains the position of record in the related table.

```php
<?php
use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Model\EntityRelationship\TableIterator;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);

$instructionFileRelation = '/project-root/my-table-instruction.php';
$tableRelation = Table::newFromInstructionsFile($instructionFileRelation);

# define record
$record = [
    'ID' => 5,
    'Profit' => 51.21,
    'ProductType' => 3,
    'ProductTypeReference' => 0
];
# find related record
$tableRelation->iterate(function($relation, TableIterator $iterator) use (&$record) {
    if ($relation['ID'] === $record['ProductType']) {
        $record['ProductTypeReference'] = $iterator->position() - 1;
        return Table::OPERATION_READ_STOP;
    }
});
# create
$table->create($record);
```

In the above example we link product type id with actual reference to relation table.

Now it's very easy (AND fast) to read the related record

```php
<?php
use DataManagement\Model\EntityRelationship\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);

$instructionFileRelation = '/project-root/my-table-instruction.php';
$tableRelation = Table::newFromInstructionsFile($instructionFileRelation);

# read the record
$record = $table->read(function($record) { if ($record['ID'] === 5) { return Table::OPERATION_READ_INCLUDE_AND_STOP; } })[0];

# read the related record with 1 (!) operation
$iterator = $tableRelation->newIterator();
$iterator->jump($record['ProductTypeReference']);
$productTypeRecord = $iterator->read();
```

## Columnar tables functionality

Columnar tables are distributed into columns, each column is placed in a separate file.

All columns are partitioned by partitioning column. This column is necessary to define to proceed.

Records are written into temporary files first and are not available until merged into a partition.

Command to trigger the merge
```bin/dmc dmc:col:table-merge <table-instructions-file>```

Columnar tables are placed into a directory.

All the records are compressed on write. Compression level can be set from 1 to 9 (the higher - the more compression)

### Columnar table operations

Operations that are supported
* create record
* read record

#### Create record
 ```php
 <?php
 
use DataManagement\Model\Columnar\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);

# define records
$records = [];
$records[] = [
    'ID' => 1,
    'Date' => '2016-01-02'
];
$records[] = [
    'ID' => 2,
    'Date' => '2016-02-02'
];
# create
$table->create($records);
 ```
 
Important notes:
* each write creates a new file (for each column)
* it is best to write in bigger chunks

#### Read record
 ```php
 <?php
 
use DataManagement\Model\Columnar\Table;

$instructionFile = '/project-root/my-table-instruction.php';
$table = Table::newFromInstructionsFile($instructionFile);

# using iterator
$result = [];
$table->iterate(
    ['ID'], # columns
    function($record) use (&$result) { # search callback
        $result[] = $record;
    }
);

# using read operator
$result = $table->read(
    ['ID', 'Date'], # columns
    function(array $record){ # search callback
        if ($record['ID'] === 5) {
            return Table::OPERATION_READ_INCLUDE_AND_STOP;
        }
    }
);
 ```
 
Important notes:
* you need to define column to read
* only specified column will be returned