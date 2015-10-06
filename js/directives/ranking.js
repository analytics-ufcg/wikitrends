angular.module('wikitrends').directive('ranking', function ($parse) {
    var directiveDefinitionObject = {
        template: "<div class='rChart nvd3'>",
        restrict: 'E',
        replace: false,
        scope: {
            ranking: '=rankingData'
        },
        link: function (scope, element, attrs) {

            scope.$watch('ranking', function (oldValue, newValue) {
                if (scope.ranking && scope.ranking.length > 0) {
                    scope.plotdata = [{
                        "key": attrs.rankingTitle,
                        "color": "#d67777",
                        "values": scope.ranking
                    }];

                    scope.redraw();
                }
            });

            scope.redraw = function () {


                nv.addGraph(function () {
                    var chart = nv.models.multiBarHorizontalChart()
                        .width(800)
                        .height(500)
                        .x(function (d) {
                            return d[attrs.rankingLabel]
                        })
                        .y(function (d) {
                            return parseInt(d[attrs.rankingMetric], 10)
                        })
                        .margin({
                            top: 30,
                            right: 20,
                            bottom: 50,
                            left: 275
                        })
                        .showControls(false);

                    chart.yAxis
                        .tickFormat(d3.format('d'));

                    d3.select(element[0]).select('div')
                        .append('svg')
                        .datum(scope.plotdata)
                        .call(chart);

                    nv.utils.windowResize(chart.update);

                    return chart;
                });

                return;
            };
        }
    }
    return directiveDefinitionObject;
});