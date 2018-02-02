<?php

function author_details($id) {
	$db = new SQLite3('swiatowid.sqlite');
	$statement = $db->prepare('SELECT * FROM v WHERE authorId=:id ORDER BY year DESC');
	$statement->bindValue(':id', $id);
	$result = $statement->execute();

	$articles="<table class='row-borders'>";
	$articles.="<tr><td>Punkty<td>Lista<td>Czasopismo<td>Rok<td>Tytuł<td>Autorzy";
	while ($row = $result->fetchArray(SQLITE3_ASSOC)) {
		extract($row);
	 	$articles.="<tr><td align=right>$points<td>$letter<td>$parentTitle<td>$year<td>$title<td>$authors";
	}
	$articles.="</table>";
	echo $articles;
}

#print_r( author_details('1760680')); 
if(isset($_GET['author_details'])) { author_details($_GET['author_details']); }

?>
