<?php

function author_details() {
	$str = file_get_contents('authors_details.json');
	$json = json_decode($str, true);
	echo json_encode($json[$_GET['author_details']]);
}

if(isset($_GET['author_details'])) { author_details(); }

?>
