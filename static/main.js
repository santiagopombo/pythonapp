// main.js


        
$(document).ready(function() {
	var t = $('#messages').DataTable();
	$.get('/received',function(data, status) {
		console.log(data);
		var obj = $.parseJSON(data);
		
		$.each(obj, function() {
			t.row.add([this['sequence_id'],this['sequence_value'],this['created_date']]).draw();
			});
					
	});
	
	console.log("ready")
});



