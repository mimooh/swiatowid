$("body").on("click", "author-details",function() {
    $(this).fadeOut(); 
});

$("body").on("click", "author",function() {
	$.ajax({
		url: "ajax.php",
		dataType: "html",
		data: { author_details: $(this).attr('id') }
	})
	.done(function(data) {
        $("author-details").html(data);
        $("author-details").fadeIn();
	});
});

