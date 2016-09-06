define([], function () {
    function controller($scope, $service, rdfService) {

        $scope.dialog = {
            'rewriteCache': '',
            'interval': '',
            'registry': ''
        };

        var rdf = rdfService.create('http://data.gov.cz/resource/lp/etl/components/e-portal-gov-cz/');

        function loadDialog() {
            rdf.setData($service.config.instance);
            var resource = rdf.secureByType('Configuration');

            $scope.dialog.rewriteCache = rdf.getBoolean(resource, 'rewriteCache');
            $scope.dialog.interval = rdf.getInteger(resource, 'interval');
            $scope.dialog.registry = rdf.getInteger(resource, 'registry');
        };

        function saveDialog() {
            var resource = rdf.secureByType('Configuration');

            rdf.setBoolean(resource, 'rewriteCache', $scope.dialog.rewriteCache);
            rdf.setInteger(resource, 'interval', $scope.dialog.interval);
            rdf.setInteger(resource, 'registry', $scope.dialog.registry);

            return rdf.getData();
        };

        $service.onStore = function () {
            saveDialog();
        };

        // Load data.
        loadDialog();
    }
    //
    controller.$inject = ['$scope', '$service', 'services.rdf.0.0.0'];
    return controller;
});