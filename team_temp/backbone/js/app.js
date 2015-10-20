var directory = {

	views : {},

	models : {},

	loadTemplates : function(views, callback) {

		var deferreds = [];
		console.log(views);

		$.each(views, function(index, view) {

			if (directory[view]) {
				deferreds.push($.get('backbone/tmpl/' + view + '.html', function(data) {
					directory[view].prototype.template = _.template(data);
				}, 'html'));
			} else {
				alert(view + " not found");
			}
		});

		$.when.apply(null, deferreds).done(callback);
	}
};

directory.Router = Backbone.Router.extend({

	initialize : function() {

		directory.shellView = new directory.ShellView();

		$('body').html(directory.shellView.render().el);
		$(".dropdown-toggle").dropdown();

		directory.flowchartView = new directory.FlowChartView();
		$("#div_flowchart").html(directory.flowchartView.render().el);

		directory.coursesView = new directory.CoursesView();
		$("#main").append(directory.coursesView.render().el);

		init_jsplumb();
		directory.coursesView.setPositions2("http://analytics.lsd.ufcg.edu.br/ccc/disciplinasPorPeriodo", 0);
		
		initModal();
	}
});

$(document).on("ready", function() {
	directory.loadTemplates(["ShellView", "FlowChartView", "CoursesView"], function() {
		directory.router = new directory.Router();
		Backbone.history.start();
	});
});