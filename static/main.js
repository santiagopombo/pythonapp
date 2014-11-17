// main.js


        
$(document).ready(function() {
	$('#submit_button').click(function(e) {
		if($("optionsRadios1").prop("checked")){
			var url = "/send";
		}
		else{
			var url = "/fib";
		}
		$.ajax({
		  type: "POST",
		  url: url,
		  data: $("#gen_fib").val(),
		  success: function(data){alert(data)}
		});

		
	});
	
});



