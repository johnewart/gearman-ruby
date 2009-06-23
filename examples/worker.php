<?php
error_reporting(E_ALL & ~E_NOTICE);

require_once 'Net/Gearman/Worker.php';

try {
    $worker = new Net_Gearman_Worker(array('localhost:4730'));
    $worker->addAbility('Sleep');
    $worker->beginWork();
} catch (Net_Gearman_Exception $e) {
    echo $e->getMessage() . "\n";
    exit;
}

?>