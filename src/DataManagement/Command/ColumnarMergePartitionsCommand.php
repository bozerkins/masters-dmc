<?php

namespace DataManagement\Command;

use DataManagement\Model\Columnar\Table;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\Input;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ColumnarMergePartitionsCommand extends Command
{
    protected function configure()
    {
        $this
            ->setName('dmc:col:table-merge')
            ->addArgument('table',  InputArgument::REQUIRED, 'Location of instructions file for the table')
            ->addOption('partition', null, InputOption::VALUE_OPTIONAL, 'specify partition to merge')
        ;
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int|null|void
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $table = Table::newFromInstructionsFile($input->getArgument('table'));

        if ($input->getOption('partition') === null) {
            foreach($table->partitions() as $partition) {
                $table->merge($partition);
            }
            return;
        }
        if (in_array($input->getOption('partition'), $table->partitions()) === false) {
            throw new \Exception('partition not found');
        }
        $table->merge($input->getOption('partition'));
    }
}