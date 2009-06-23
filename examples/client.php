<?php

require_once 'Net/Gearman/Client.php';

$set = new Net_Gearman_Set();

function result($func, $handle, $result) {
    var_dump($func);
    var_dump($handle);
    var_dump($result);
}

$task = new Net_Gearman_Task('Sleep', array(
  'seconds' => 20
));

$task->attachCallback('result');
$set->addTask($task);

$client = new Net_Gearman_Client(array('localhost:4730', 'localhost:4731'));
$client->runSet($set);

?>
