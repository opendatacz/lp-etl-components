define([], function () {
    function controller($scope, $service, rdfService) {

        $scope.dialog = { };

        var rdf = rdfService.create('http://plugins.linkedpipes.com/ontology/x-ckanPurger#');

        function loadDialog() {
            rdf.setData($service.config.instance);
            var resource = rdf.secureByType('Configuration');

            $scope.dialog.apiUrl = rdf.getString(resource, 'apiUrl');
            $scope.dialog.apiKey = rdf.getString(resource, 'apiKey');

            $scope.dialog.purgeAllDatasets = rdf.getBoolean(resource, 'purgeAllDatasets');
            $scope.dialog.purgeAllOrganizations = rdf.getBoolean(resource, 'purgeAllOrganizations');
            $scope.dialog.failOnError = rdf.getBoolean(resource, 'failOnError');
        };

        function saveDialog() {
            var resource = rdf.secureByType('Configuration');

            rdf.setString(resource, 'apiUrl', $scope.dialog.apiUrl);
            rdf.setString(resource, 'apiKey', $scope.dialog.apiKey);

            rdf.setBoolean(resource, 'purgeAllDatasets', $scope.dialog.purgeAllDatasets);
            rdf.setBoolean(resource, 'purgeAllOrganizations', $scope.dialog.purgeAllOrganizations);
            rdf.setBoolean(resource, 'failOnError', $scope.dialog.failOnError);

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