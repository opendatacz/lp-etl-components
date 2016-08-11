define([], function () {
    function controller($scope, rdfService) {

        $scope.dialog = { };

        var rdf = rdfService.create('http://plugins.linkedpipes.com/ontology/l-dcatAp11ToDkanBatch#');

        $scope.profiles = [
            {
                "@id": "http://plugins.etl.linkedpipes.com/resource/l-dcatAp11ToDkanBatch/profiles/DKAN",
                "http://www.w3.org/2004/02/skos/core#prefLabel": [
                    {
                        "@language": "en",
                        "@value": "Pure DKAN"
                    }
                ]
            },
            {
                "@id": "http://plugins.etl.linkedpipes.com/resource/l-dcatAp11ToDkanBatch/profiles/CZ-NKOD",
                "http://www.w3.org/2004/02/skos/core#prefLabel": [
                    {
                        "@language": "en",
                        "@value": "Czech NKOD"
                    }
                ]
            }
        ];

        $scope.setConfiguration = function (inConfig) {
            rdf.setData(inConfig);
            var resource = rdf.secureByType('Configuration');

            $scope.dialog.apiUrl = rdf.getString(resource, 'apiUrl');
            $scope.dialog.username = rdf.getString(resource, 'username');
            $scope.dialog.password = rdf.getString(resource, 'password');
            $scope.dialog.loadLanguage = rdf.getString(resource, 'loadLanguage') ;
            $scope.dialog.profile = rdf.getString(resource, 'profile') ;
        };

        $scope.getConfiguration = function () {
            var resource = rdf.secureByType('Configuration');

            rdf.setString(resource, 'apiUrl', $scope.dialog.apiUrl);
            rdf.setString(resource, 'username', $scope.dialog.username);
            rdf.setString(resource, 'password', $scope.dialog.password);
            rdf.setString(resource, 'loadLanguage', $scope.dialog.loadLanguage);
            rdf.setString(resource, 'profile', $scope.dialog.profile);

            return rdf.getData();
        };
    }
    //
    controller.$inject = ['$scope', 'services.rdf.0.0.0'];
    return controller;
});