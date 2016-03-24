define([], function () {
    function controller($scope, rdfService) {

        $scope.dialog = {
            'rewriteCache': '',
            'interval': '',
            'registry': ''
        };

        var rdf = rdfService.create('http://data.gov.cz/resource/lp/etl/components/e-portal-gov-cz/');

        $scope.setConfiguration = function (inConfig) {
            rdf.setData(inConfig);
            var resource = rdf.secureByType('Configuration');

            $scope.dialog.rewriteCache = rdf.getBoolean(resource, 'rewriteCache');
            $scope.dialog.interval = rdf.getInteger(resource, 'interval');
            $scope.dialog.registry = rdf.getInteger(resource, 'registry');
        };

        $scope.getConfiguration = function () {
            var resource = rdf.secureByType('Configuration');

            rdf.setBoolean(resource, 'rewriteCache', $scope.dialog.rewriteCache);
            rdf.setInteger(resource, 'interval', $scope.dialog.interval);
            rdf.setInteger(resource, 'registry', $scope.dialog.registry);

            return rdf.getData();
        };
    }
    //
    controller.$inject = ['$scope', 'services.rdf.0.0.0'];
    return controller;
});