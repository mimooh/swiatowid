$.getJSON("plot_data.json", function(authors) {
	plot(authors);
});

$("body").on("click", "author",function() {
	console.log($(this).attr('id'));
	$.ajax({
		url: "ajax.php",
		dataType: "json",
		data: { author_details: $(this).attr('id') }
	})
	.done(function(data) {
		console.log(data);
	});
});

function plot(authors) { 
	$("body").append('<table id=plot_table>');
	
	for (var i=0; i<authors.length; i++) { 
		$("#plot_table").append('<tr><td>'+(i+1)+'<td><author id='+authors[i]['authorId']+'>'+authors[i]['familyName']+' '+authors[i]['givenNames']+'</author><td>'+authors[i]['pointsShare']+'<td><svg width="1000" height="20" id=svg'+i+'></svg>');
		Pablo("#svg"+i).append('<rect y="0" x="0" height="20" width="'+authors[i]['pointsShare']+'" id="rect'+i+'" style="color:#000000; opacity:0.8; fill:#004488; stroke:#0088ff; stroke-width:1" /> ');
	}
}
