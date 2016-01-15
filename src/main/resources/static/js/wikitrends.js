var app = angular.module('wikitrends', ['ngRoute', 'pascalprecht.translate', 'tmh.dynamicLocale']);

app.config(function ($routeProvider) {
    $routeProvider
        .when('/', {
            templateUrl: 'templates/pages/static/index.html',
            controller: 'StaticController',
            controllerAs: 'staticController'

        })
        .otherwise({
            redirectTo: '/'
        });
});

app.controller('StaticController', function ($scope, $http) {
    $http.get('statistics').then(function (response) {
        response.data.forEach(function (d) {
            $scope[d.key] = parseInt(d.value, 10)
        })
        $scope.edits = [{
            label: "Major Edits",
            value: $scope['all_edits'] - $scope['minor_edits']
        }, {
            label: "Minor Edits",
            value: $scope['minor_edits']
        }]
    }, function (response) {
        console.log(response.data);
    });

    $http.get('v2/editors', {
        params: { 
            size: 20
        }
    }).then(function (response) {
        $scope.editors = response.data.map(function(d){
            return {
                user: d.key,
                count: d.value
            }
        })
    }, function (response) {
        console.log(response.data);
    });

    $http.get('v2/idioms', {
        params: { 
            size: 20 
        }
    }).then(function (response) {
        $scope.idioms = response.data.map(function(d){
            return {
                idiom: d.key,
                count: d.value
            }
        })
    }, function (response) {
        console.log(response.data);
    });

    $http.get('v2/pages', {
        params: { 
            size: 20 
        }
    }).then(function (response) {
        $scope.pages = response.data.map(function(d){
            return {
                page: d.key,
                count: d.value
            }
        })
    }, function (response) {
        console.log(response.data);
    });

    $http.get('v2/pages', {
        params: { 
            contentonly: true,
            size: 20 
        }
    }).then(function (response) {
        $scope.pages_content = response.data.map(function(d){
            return {
                page: d.key,
                count: d.value
            }
        })
    }, function (response) {
        console.log(response.data);
    });

});

app.filter('round', function(){

    return function(n){
        return Math.round(n);
    };
});

app.filter('bytes', function() {
    return function(bytes, precision) {
        if (isNaN(parseFloat(bytes)) || !isFinite(bytes)) return '-';
        if (typeof precision === 'undefined') precision = 1;
        var units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'],
            number = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +  ' ' + units[number];
    }
});

