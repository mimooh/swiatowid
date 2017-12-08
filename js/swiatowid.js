$.getJSON("toPlot.json", function(authors) {
	plot(authors);
});

function plot(authors) { 
	//$("body").html('<svg width="100" height="5000px"></svg>');
	$("body").html('<table border=1 id=plot_table>');
	
	for (var i=0; i<50; i++) { 
		$("#plot_table").append('<tr><td>nazwisko<td><svg width="1000" height="20" id=svg'+i+'></svg>');
		$("#plot_table").append(Pablo("#svg"+i).append('<rect y="0" x="0" height="20" width="800" id="rect'+i+'" style="color:#000000; opacity:0.5; fill:#ff0000;" />'));
	}
}
