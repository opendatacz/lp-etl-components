define([], function () {
    function controller($scope, rdfService) {

        $scope.dialog = {
        };

        var rdf = rdfService.create('http://plugins.linkedpipes.com/ontology/t-fdpToRdf#');

        $scope.setConfiguration = function (inConfig) {
            rdf.setData(inConfig);
            var resource = rdf.secureByType('Configuration');
        };

        $scope.getConfiguration = function () {
            var resource = rdf.secureByType('Configuration');
            return rdf.getData();
        };
    }
    //
    controller.$inject = ['$scope', 'services.rdf.0.0.0'];
    return controller;
});