#!/usr/bin/php
<?php

$last_word = "";
$last_count = 0;

while (($line = fgets(STDIN)) !== false) {
    $words = explode("\t", trim($line)); 
    $word = $words[0];
    $count = intval($words[1]);

    if($word == $last_word) {
        $last_count += $count;
    } else {
        if ($last_word != "") {
            echo "$last_word\t$last_count\n";
	}
        $last_word = $word;
	$last_count = $count;
    }
}//end while
        if ($last_word != "") {
            echo "$last_word\t$last_count\n";
	}
?>
