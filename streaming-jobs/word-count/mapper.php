#!/usr/bin/php
<?php
while (($line = fgets(STDIN)) !== false) {
    $words = explode(" ", trim($line)); 
    foreach($words as $word) {
        echo "$word\t1\n";
    }
}//end while
?>
