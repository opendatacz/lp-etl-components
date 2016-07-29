define([], function () {
    function controller($scope, rdfService) {

        $scope.dialog = { };

        var rdf = rdfService.create('http://plugins.linkedpipes.com/ontology/l-dcatAp11ToCkanBatch#');

        $scope.profiles = [
            {
                "@id": "http://plugins.etl.linkedpipes.com/resource/l-dcatAp11ToCkanBatch/profiles/CKAN",
                "http://www.w3.org/2004/02/skos/core#prefLabel": [
                    {
                        "@language": "en",
                        "@value": "Pure CKAN"
                    }
                ]
            },
            {
                "@id": "http://plugins.etl.linkedpipes.com/resource/l-dcatAp11ToCkanBatch/profiles/CZ-NKOD",
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
            $scope.dialog.apiKey = rdf.getString(resource, 'apiKey');
            $scope.dialog.loadLanguage = rdf.getString(resource, 'loadLanguage') ;
            $scope.dialog.profile = rdf.getString(resource, 'profile') ;
        };

        $scope.getConfiguration = function () {
            var resource = rdf.secureByType('Configuration');

            rdf.setString(resource, 'apiUrl', $scope.dialog.apiUrl);
            rdf.setString(resource, 'apiKey', $scope.dialog.apiKey);
            rdf.setString(resource, 'loadLanguage', $scope.dialog.loadLanguage);
            rdf.setString(resource, 'profile', $scope.dialog.profile);

            return rdf.getData();
        };
    }
    //
    controller.$inject = ['$scope', 'services.rdf.0.0.0'];
    return controller;
});