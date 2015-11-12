angular.module('wikitrends').directive('wikiPie', function ($parse) {
    var directiveDefinitionObject = {
        template: "<div class='rChart nvd3'>",
        restrict: 'E',
        replace: false,
        scope: {
            pieData: '=pieData'
        },
        link: function (scope, element, attrs) {

            scope.$watch('pieData', function (oldValue, newValue) {
                if (scope.pieData && scope.pieData.length > 0) {
                    scope.redraw();
                }
            });

            scope.redraw = function () {
                nv.addGraph(function() {
                    var chart = nv.models.pieChart()
                    .x(function(d) { return d.label })
                    .y(function(d) { return d.value })
                    .showLabels(true)
                    .showLegend(false)
                    .noData("Oops! Something is missing...")
                    .color(["#00CC55","#CC0000"])
                    .margin({
                        top: 20,
                        right: 20,
                        bottom: 20,
                        left: 20
                    })

                    d3.select(element[0]).select('div')
                        .append('svg')
                        .datum(scope.pieData)
                        .transition().duration(350)
                        .call(chart)
                    

                    return chart;
                });                
                return;
            };
        }
    }
    return directiveDefinitionObject;
}); 