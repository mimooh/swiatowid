<?php

function author_details($id) {
	$db = new SQLite3('swiatowid.sqlite');
	$statement = $db->prepare('SELECT * FROM v where authorId=:id;');
	$statement->bindValue(':id', $id);
	$result = $statement->execute();

	$articles=[];
	while ($row = $result->fetchArray()) {
	 	$articles[]=$row;
	}
	echo json_encode($articles);
}

#print_r( author_details('1760680')); 
if(isset($_GET['author_details'])) { author_details($_GET['author_details']); }

?>
