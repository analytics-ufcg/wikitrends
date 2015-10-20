function gerar_conexoes()
{
	var a = new Array();	
	d3.csv("data/prereq.csv", function(data) {
		a = data;
	});
	return a;

	
};

