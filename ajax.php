<?php

function author_details() {
	$db = new SQLite3('swiatowid.sqlite');
	$statement = $db->prepare('SELECT * FROM v where authorId= :id;');
	$statement->bindValue(':id', intval($_GET['author_details']));
	$result = $statement->execute();

	$articles=[];
	while ($row = $result->fetchArray()) {
	 	$articles[]=$row;
	}
	echo json_encode($articles);
}

if(isset($_GET['author_details'])) { author_details(); }

?>
