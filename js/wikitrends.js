angular.module('wikitrends', ['ngRoute']);

angular.module('wikitrends').config(function ($routeProvider) {
    $routeProvider
        .when('/', {
            templateUrl: '/templates/pages/static/index.html',
            controller: 'StaticController',
            controllerAs: 'staticController'

        })
        .otherwise({
            redirectTo: '/'
        });
});

angular.module('wikitrends').controller('StaticController', function ($scope, $http) {
    $scope.deputados = [];

    $http.get('data/absolute.tsv').then(function (response) {
        d3.tsv.parse(response.data).forEach(function (d) {
            $scope[d.field] = d.count
        })
    }, function (response) {
        console.log(response.data);
    });

    $http.get('data/editors.tsv').then(function (response) {
        $scope.editors = d3.tsv.parse(response.data)
    }, function (response) {
        console.log(response.data);
    });

    $http.get('data/idioms.tsv').then(function (response) {
        $scope.idioms = d3.tsv.parse(response.data)
        console.log($scope.idioms)
    }, function (response) {
        console.log(response.data);
    });

    $http.get('data/pages.tsv').then(function (response) {
        $scope.pages = d3.tsv.parse(response.data)
    }, function (response) {
        console.log(response.data);
    });

});