<?php

/**
 * Sum up a bunch of numbers
 *
 * @author      Ladislav Martincik <ladislav.martincik@xing.com>
 * @package     Net_Gearman
 */
class Net_Gearman_Job_Sleep extends Net_Gearman_Job_Common
{
  /**
   * Run the Sleep job
   *
   * @access      public
   * @param       array       $arg
   * @return      array
   */
  public function run($arg)
  {
    $seconds = $arg['seconds'];
    echo $seconds;
    while ($seconds > 0) {
      print $seconds;
      sleep(1);
      $this->status($i, $seconds);
      $seconds--;
    }

    return true;
  }
}

?>