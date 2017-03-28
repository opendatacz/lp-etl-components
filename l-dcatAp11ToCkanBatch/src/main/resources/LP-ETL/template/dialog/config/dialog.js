define([], function () {
    "use strict";

    const DESC = {
        "$namespace": "http://plugins.linkedpipes.com/ontology/l-dcatAp11ToCkanBatch#",
        "$type": "Configuration",
        "$options": {
            "$predicate": "auto",
            "$control": "auto"
        },
        "apiUrl": {
            "$type": "str",
            "$label": "CKAN Action API URL"
        },
        "apiKey": {
            "$type": "str",
            "$label": "CKAN API Key"
        },
        "loadLanguage": {
            "$type": "str",
            "$label": "Load language"
        },
        "profile": {
            "$type": "str",
            "$label": "Profile"
        },
        "toFile": {
            "$type": "bool",
            "$label": "Output a JSON Lines file"
        },
        "toApi": {
            "$type": "bool",
            "$label": "Load to CKAN API"
        }
    };

    const PROFILES = [
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

    function controller($scope, $service) {

        if ($scope.dialog === undefined) {
            $scope.dialog = {};
        }

        const dialogManager = $service.v1.manager(DESC, $scope.dialog);

        $service.onStore = function () {
            dialogManager.save();
        };

        $scope.profiles = PROFILES ;

        dialogManager.load();
    }
    //
    controller.$inject = ['$scope', '$service'];
    return controller;
});