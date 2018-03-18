<?php

namespace DataManagement\Command;

use DataManagement\Model\EntityRelationship\Table;
use DataManagement\Storage\FileStorage;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\Input;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class EntityRelationshipTableOptimizeCommand extends Command
{
    protected function configure()
    {
        $this
            ->setName('dmc:er:table-optimize')
            ->addArgument('table',  InputArgument::REQUIRED, 'Location of instructions file for the table')
            ->addOption('analyze', null, InputOption::VALUE_NONE, 'Show only active records')
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
        if ($input->getOption('analyze')) {
            $total = $table->amountOfRecords();
            $totalActive = $table->amountOfActiveRecords();
            $wastePercentage = round(($totalActive / $total) * 100, 2);
            $output->writeln('Total records: ' . $total);
            $output->writeln('Total active records: ' . $totalActive);
            $output->writeln('Waste percentage: ' . $wastePercentage . '%');
            $output->writeln($wastePercentage > 70 ? 'optimization not required' : 'optimization required');
            return;
        }
        $tableTemporary = new Table(new FileStorage(tempnam('/tmp', 'temp_dmc_er_table_')));
        $tableTemporary->load($table->structure());
        $table->iterate(function($record) use ($tableTemporary) {
            $tableTemporary->create($record);
        });
        $table->storage()->remove();
        $tableTemporary->storage()->move($table->storage()->file());
    }
}