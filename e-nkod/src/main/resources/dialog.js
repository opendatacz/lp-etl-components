define([], function () {
    function controller($scope, rdfService) {

        $scope.dialog = {
            'rewriteCache': '',
            'interval': ''
        };

        var rdf = rdfService.create('http://data.gov.cz/resource/lp/etl/components/e-nkod/');

        $scope.setConfiguration = function (inConfig) {
            rdf.setData(inConfig);
            var resource = rdf.secureByType('Configuration');

            $scope.dialog.rewriteCache = rdf.getBoolean(resource, 'rewriteCache');
            $scope.dialog.interval = rdf.getInteger(resource, 'interval');
        };

        $scope.getConfiguration = function () {
            var resource = rdf.secureByType('Configuration');

            rdf.setBoolean(resource, 'rewriteCache', $scope.dialog.rewriteCache);
            rdf.setInteger(resource, 'interval', $scope.dialog.interval);

            return rdf.getData();
        };
    }
    //
    controller.$inject = ['$scope', 'services.rdf.0.0.0'];
    return controller;
});