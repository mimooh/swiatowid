$.getJSON("plot_data.json", function(authors) {
	plot(authors);
});

function plot(authors) { 
	$("body").append('<table id=plot_table>');
	
	for (var i=0; i<authors.length; i++) { 
		$("#plot_table").append('<tr><td>'+authors[i]['familyName']+' '+authors[i]['givenNames']+'<td>'+authors[i]['points']+'<td><svg width="1000" height="20" id=svg'+i+'></svg>');
		Pablo("#svg"+i).append('<rect y="0" x="0" height="20" width="'+authors[i]['points']+'" id="rect'+i+'" style="color:#000000; opacity:0.8; fill:#004488; stroke:#0066aa; stroke-width:1" /> ');
	}
}
