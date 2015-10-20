
directory.Course = Backbone.Model.extend({
    defaults: {
        periodo: 1,
        disciplina: "none",
        codigo: 1
    }

});

// A List of People
directory.CourseCollection = Backbone.Collection.extend({
    model: directory.Course
});

function readJSON(url){
    	var dataframe;

		$.ajax({
            url : url,
            //url : "http://analytics.lsd.ufcg.edu.br/ccc/getDisciplinasPorPeriodo",
            type : 'GET',
            async: false,
            dataType : 'json',
            success: function(data) { 
            	console.log("success ajax!");
            	//console.log(data);

            	dataframe = data;
             },                                                                                                                                                                                       
            error: function() { console.log('error ajax!'); },
        });

        return dataframe;
}



